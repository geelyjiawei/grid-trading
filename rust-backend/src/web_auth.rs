use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::http::{HeaderMap, header::COOKIE};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use data_encoding::BASE32_NOPAD;
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use serde::Serialize;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;
use zeroize::Zeroizing;

pub const SESSION_COOKIE_NAME: &str = "grid_session";
pub const SESSION_TTL_SECONDS: u64 = 12 * 60 * 60;

const PASSWORD_HASH_SCHEME: &str = "pbkdf2_sha256";
const MIN_PBKDF2_ITERATIONS: u32 = 100_000;
const MAX_PBKDF2_ITERATIONS: u32 = 2_000_000;
const PASSWORD_DIGEST_BYTES: usize = 32;
const MIN_TOTP_SECRET_BYTES: usize = 10;
const MAX_TOTP_SECRET_BYTES: usize = 128;
const SESSION_TOKEN_BYTES: usize = 32;
const SESSION_TOKEN_ATTEMPTS: usize = 4;
const MAX_ACTIVE_SESSIONS: usize = 128;
const MAX_COOKIE_HEADER_BYTES: usize = 8_192;
const MAX_USERNAME_BYTES: usize = 128;
const MAX_PASSWORD_BYTES: usize = 1_024;
const LOGIN_FAILURE_WINDOW_SECONDS: u64 = 60;
const MAX_LOGIN_FAILURES_PER_WINDOW: usize = 8;

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
pub struct WebAuthStatus {
    pub required: bool,
    pub configured: bool,
    pub authenticated: bool,
    pub username: Option<String>,
}

pub struct WebAuthConfiguration {
    pub required: bool,
    pub username: String,
    pub password_hash: Zeroizing<String>,
    pub totp_secret: Zeroizing<String>,
    pub cookie_secure: bool,
}

impl fmt::Debug for WebAuthConfiguration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebAuthConfiguration")
            .field("required", &self.required)
            .field("username", &self.username)
            .field("password_hash", &"[REDACTED]")
            .field("totp_secret", &"[REDACTED]")
            .field("cookie_secure", &self.cookie_secure)
            .finish()
    }
}

#[derive(Clone)]
pub struct WebAuthService {
    inner: Arc<WebAuthInner>,
}

struct WebAuthInner {
    required: bool,
    credentials: Option<Credentials>,
    cookie_secure: bool,
    sessions: Mutex<HashMap<[u8; 32], SessionRecord>>,
    throttle: Mutex<LoginThrottle>,
    clock: Arc<dyn Clock>,
    token_source: Arc<dyn SessionTokenSource>,
}

struct Credentials {
    username: String,
    username_digest: [u8; 32],
    password: PasswordVerifier,
    totp: TotpVerifier,
}

struct PasswordVerifier {
    iterations: u32,
    salt: String,
    expected: [u8; PASSWORD_DIGEST_BYTES],
}

struct TotpVerifier {
    secret: Zeroizing<Vec<u8>>,
}

struct SessionRecord {
    username: String,
    expires_at: u64,
}

#[derive(Default)]
struct LoginThrottle {
    failures: VecDeque<u64>,
}

trait Clock: Send + Sync {
    fn now_seconds(&self) -> Result<u64, WebAuthUnavailable>;
}

struct SystemClock;

impl Clock for SystemClock {
    fn now_seconds(&self) -> Result<u64, WebAuthUnavailable> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .map_err(|_| WebAuthUnavailable::Clock)
    }
}

trait SessionTokenSource: Send + Sync {
    fn fill(&self, output: &mut [u8]) -> Result<(), WebAuthUnavailable>;
}

struct OsSessionTokenSource;

impl SessionTokenSource for OsSessionTokenSource {
    fn fill(&self, output: &mut [u8]) -> Result<(), WebAuthUnavailable> {
        getrandom::fill(output).map_err(|_| WebAuthUnavailable::Entropy)
    }
}

pub enum WebLoginOutcome {
    AuthenticationDisabled,
    Authenticated { session_token: Zeroizing<String> },
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum WebLoginError {
    #[error("authentication is required but not configured")]
    NotConfigured,
    #[error("invalid username, password, or code")]
    InvalidCredentials,
    #[error("too many login attempts")]
    RateLimited,
    #[error("authentication service is unavailable: {0}")]
    Unavailable(WebAuthUnavailable),
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum WebAuthorizationError {
    #[error("authentication is required but not configured")]
    NotConfigured,
    #[error("authentication required")]
    NotAuthenticated,
    #[error("authentication service is unavailable: {0}")]
    Unavailable(WebAuthUnavailable),
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WebAuthConfigurationError {
    #[error("administrator username is invalid")]
    InvalidUsername,
    #[error("administrator password hash is invalid")]
    InvalidPasswordHash,
    #[error("TOTP secret is invalid")]
    InvalidTotpSecret,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum WebAuthUnavailable {
    #[error("system clock is unavailable")]
    Clock,
    #[error("secure random source is unavailable")]
    Entropy,
    #[error("authentication session store is unavailable")]
    SessionStore,
}

impl WebAuthService {
    pub fn from_configuration(
        configuration: WebAuthConfiguration,
    ) -> Result<Self, WebAuthConfigurationError> {
        Self::from_configuration_with_dependencies(
            configuration,
            Arc::new(SystemClock),
            Arc::new(OsSessionTokenSource),
        )
    }

    pub fn disabled() -> Self {
        Self::from_configuration(WebAuthConfiguration {
            required: false,
            username: String::new(),
            password_hash: Zeroizing::new(String::new()),
            totp_secret: Zeroizing::new(String::new()),
            cookie_secure: false,
        })
        .expect("an empty, disabled authentication configuration is valid")
    }

    fn from_configuration_with_dependencies(
        configuration: WebAuthConfiguration,
        clock: Arc<dyn Clock>,
        token_source: Arc<dyn SessionTokenSource>,
    ) -> Result<Self, WebAuthConfigurationError> {
        let username = configuration.username.trim().to_owned();
        let password_hash = configuration.password_hash.trim();
        let totp_secret = configuration.totp_secret.trim();
        let credentials =
            if username.is_empty() || password_hash.is_empty() || totp_secret.is_empty() {
                None
            } else {
                Some(Credentials::parse(&username, password_hash, totp_secret)?)
            };

        Ok(Self {
            inner: Arc::new(WebAuthInner {
                required: configuration.required,
                credentials,
                cookie_secure: configuration.cookie_secure,
                sessions: Mutex::new(HashMap::new()),
                throttle: Mutex::new(LoginThrottle::default()),
                clock,
                token_source,
            }),
        })
    }

    pub fn cookie_secure(&self) -> bool {
        self.inner.cookie_secure
    }

    pub fn status(&self, headers: &HeaderMap) -> Result<WebAuthStatus, WebAuthUnavailable> {
        let configured = self.inner.credentials.is_some();
        if !configured {
            return Ok(WebAuthStatus {
                required: self.inner.required,
                configured: false,
                authenticated: !self.inner.required,
                username: None,
            });
        }

        let username = self.session_username(headers)?;
        Ok(WebAuthStatus {
            required: self.inner.required,
            configured: true,
            authenticated: username.is_some() || !self.inner.required,
            username,
        })
    }

    pub fn authorize(&self, headers: &HeaderMap) -> Result<Option<String>, WebAuthorizationError> {
        if !self.inner.required {
            return Ok(None);
        }
        if self.inner.credentials.is_none() {
            return Err(WebAuthorizationError::NotConfigured);
        }
        self.session_username(headers)
            .map_err(WebAuthorizationError::Unavailable)?
            .ok_or(WebAuthorizationError::NotAuthenticated)
            .map(Some)
    }

    pub fn login(
        &self,
        username: &str,
        password: &str,
        code: &str,
    ) -> Result<WebLoginOutcome, WebLoginError> {
        if !self.inner.required {
            return Ok(WebLoginOutcome::AuthenticationDisabled);
        }
        let Some(credentials) = &self.inner.credentials else {
            return Err(WebLoginError::NotConfigured);
        };
        let now = self
            .inner
            .clock
            .now_seconds()
            .map_err(WebLoginError::Unavailable)?;

        if self.login_is_rate_limited(now)? {
            return Err(WebLoginError::RateLimited);
        }

        let username_digest: [u8; 32] = Sha256::digest(username.as_bytes()).into();
        let username_valid = username.len() <= MAX_USERNAME_BYTES
            && bool::from(credentials.username_digest.ct_eq(&username_digest));
        let password_valid = credentials.password.verify(password);
        let totp_valid = credentials.totp.verify_at(code, now);
        if !(username_valid & password_valid & totp_valid) {
            self.record_login_failure(now)?;
            return Err(WebLoginError::InvalidCredentials);
        }

        self.clear_login_failures()?;
        let session_token = self
            .create_session(&credentials.username, now)
            .map_err(WebLoginError::Unavailable)?;
        Ok(WebLoginOutcome::Authenticated { session_token })
    }

    pub fn logout(&self, headers: &HeaderMap) -> Result<(), WebAuthUnavailable> {
        let Some(digest) = session_digest_from_headers(headers) else {
            return Ok(());
        };
        self.inner
            .sessions
            .lock()
            .map_err(|_| WebAuthUnavailable::SessionStore)?
            .remove(&digest);
        Ok(())
    }

    fn session_username(&self, headers: &HeaderMap) -> Result<Option<String>, WebAuthUnavailable> {
        let Some(digest) = session_digest_from_headers(headers) else {
            return Ok(None);
        };
        let now = self.inner.clock.now_seconds()?;
        let mut sessions = self
            .inner
            .sessions
            .lock()
            .map_err(|_| WebAuthUnavailable::SessionStore)?;
        sessions.retain(|_, record| record.expires_at > now);
        Ok(sessions.get(&digest).map(|record| record.username.clone()))
    }

    fn create_session(
        &self,
        username: &str,
        now: u64,
    ) -> Result<Zeroizing<String>, WebAuthUnavailable> {
        let expires_at = now
            .checked_add(SESSION_TTL_SECONDS)
            .ok_or(WebAuthUnavailable::Clock)?;

        for _ in 0..SESSION_TOKEN_ATTEMPTS {
            let mut token = Zeroizing::new([0_u8; SESSION_TOKEN_BYTES]);
            self.inner.token_source.fill(&mut *token)?;
            let digest: [u8; 32] = Sha256::digest(token.as_ref()).into();
            let encoded = Zeroizing::new(URL_SAFE_NO_PAD.encode(token.as_ref()));
            let mut sessions = self
                .inner
                .sessions
                .lock()
                .map_err(|_| WebAuthUnavailable::SessionStore)?;
            sessions.retain(|_, record| record.expires_at > now);
            if sessions.contains_key(&digest) {
                continue;
            }
            if sessions.len() >= MAX_ACTIVE_SESSIONS
                && let Some(oldest) = sessions
                    .iter()
                    .min_by_key(|(_, record)| record.expires_at)
                    .map(|(digest, _)| *digest)
            {
                sessions.remove(&oldest);
            }
            sessions.insert(
                digest,
                SessionRecord {
                    username: username.to_owned(),
                    expires_at,
                },
            );
            return Ok(encoded);
        }

        Err(WebAuthUnavailable::Entropy)
    }

    fn login_is_rate_limited(&self, now: u64) -> Result<bool, WebLoginError> {
        let mut throttle = self
            .inner
            .throttle
            .lock()
            .map_err(|_| WebLoginError::Unavailable(WebAuthUnavailable::SessionStore))?;
        throttle.prune(now);
        Ok(throttle.failures.len() >= MAX_LOGIN_FAILURES_PER_WINDOW)
    }

    fn record_login_failure(&self, now: u64) -> Result<(), WebLoginError> {
        let mut throttle = self
            .inner
            .throttle
            .lock()
            .map_err(|_| WebLoginError::Unavailable(WebAuthUnavailable::SessionStore))?;
        throttle.prune(now);
        throttle.failures.push_back(now);
        Ok(())
    }

    fn clear_login_failures(&self) -> Result<(), WebLoginError> {
        self.inner
            .throttle
            .lock()
            .map_err(|_| WebLoginError::Unavailable(WebAuthUnavailable::SessionStore))?
            .failures
            .clear();
        Ok(())
    }
}

impl fmt::Debug for WebAuthService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebAuthService")
            .field("required", &self.inner.required)
            .field("configured", &self.inner.credentials.is_some())
            .field("cookie_secure", &self.inner.cookie_secure)
            .field("credentials", &"[REDACTED]")
            .field("sessions", &"[REDACTED]")
            .finish()
    }
}

impl Credentials {
    fn parse(
        username: &str,
        password_hash: &str,
        totp_secret: &str,
    ) -> Result<Self, WebAuthConfigurationError> {
        if username.is_empty()
            || username.len() > MAX_USERNAME_BYTES
            || username.chars().any(char::is_control)
        {
            return Err(WebAuthConfigurationError::InvalidUsername);
        }
        Ok(Self {
            username: username.to_owned(),
            username_digest: Sha256::digest(username.as_bytes()).into(),
            password: PasswordVerifier::parse(password_hash)?,
            totp: TotpVerifier::parse(totp_secret)?,
        })
    }
}

impl PasswordVerifier {
    fn parse(stored_hash: &str) -> Result<Self, WebAuthConfigurationError> {
        if stored_hash.len() > 512 {
            return Err(WebAuthConfigurationError::InvalidPasswordHash);
        }
        let mut fields = stored_hash.split('$');
        let scheme = fields.next();
        let iterations = fields.next().and_then(|value| value.parse::<u32>().ok());
        let salt = fields.next();
        let expected = fields.next();
        if fields.next().is_some() || scheme != Some(PASSWORD_HASH_SCHEME) {
            return Err(WebAuthConfigurationError::InvalidPasswordHash);
        }
        let iterations = iterations.ok_or(WebAuthConfigurationError::InvalidPasswordHash)?;
        if !(MIN_PBKDF2_ITERATIONS..=MAX_PBKDF2_ITERATIONS).contains(&iterations) {
            return Err(WebAuthConfigurationError::InvalidPasswordHash);
        }
        let salt = salt.ok_or(WebAuthConfigurationError::InvalidPasswordHash)?;
        if !(8..=128).contains(&salt.len()) || !salt.bytes().all(is_url_safe_token_byte) {
            return Err(WebAuthConfigurationError::InvalidPasswordHash);
        }
        let expected = expected.ok_or(WebAuthConfigurationError::InvalidPasswordHash)?;
        let decoded = URL_SAFE_NO_PAD
            .decode(expected.as_bytes())
            .map_err(|_| WebAuthConfigurationError::InvalidPasswordHash)?;
        let expected: [u8; PASSWORD_DIGEST_BYTES] = decoded
            .try_into()
            .map_err(|_| WebAuthConfigurationError::InvalidPasswordHash)?;

        Ok(Self {
            iterations,
            salt: salt.to_owned(),
            expected,
        })
    }

    fn verify(&self, candidate: &str) -> bool {
        if candidate.is_empty() || candidate.len() > MAX_PASSWORD_BYTES {
            return false;
        }
        let mut actual = Zeroizing::new([0_u8; PASSWORD_DIGEST_BYTES]);
        pbkdf2_hmac::<Sha256>(
            candidate.as_bytes(),
            self.salt.as_bytes(),
            self.iterations,
            &mut *actual,
        );
        bool::from(self.expected.ct_eq(&*actual))
    }
}

impl TotpVerifier {
    fn parse(encoded_secret: &str) -> Result<Self, WebAuthConfigurationError> {
        let normalized = Zeroizing::new(
            encoded_secret
                .chars()
                .filter(|character| !character.is_ascii_whitespace())
                .map(|character| character.to_ascii_uppercase())
                .collect::<String>(),
        );
        if normalized.len() > 256 {
            return Err(WebAuthConfigurationError::InvalidTotpSecret);
        }
        let without_padding = normalized.trim_end_matches('=');
        let padding = normalized.len().saturating_sub(without_padding.len());
        if padding > 6 || without_padding.contains('=') {
            return Err(WebAuthConfigurationError::InvalidTotpSecret);
        }
        let secret = BASE32_NOPAD
            .decode(without_padding.as_bytes())
            .map_err(|_| WebAuthConfigurationError::InvalidTotpSecret)?;
        if !(MIN_TOTP_SECRET_BYTES..=MAX_TOTP_SECRET_BYTES).contains(&secret.len()) {
            return Err(WebAuthConfigurationError::InvalidTotpSecret);
        }
        Ok(Self {
            secret: Zeroizing::new(secret),
        })
    }

    fn verify_at(&self, candidate: &str, unix_seconds: u64) -> bool {
        let (candidate, shape_valid) = normalize_totp_candidate(candidate);
        let counter = unix_seconds / 30;
        let counters = [
            counter.saturating_sub(1),
            counter,
            counter.saturating_add(1),
        ];
        let mut matched = 0_u8;
        for value in counters {
            let expected = self.code(value);
            matched |= u8::from(bool::from(expected.ct_eq(&candidate)));
        }
        shape_valid && matched == 1
    }

    fn code(&self, counter: u64) -> [u8; 6] {
        let mut mac = Hmac::<Sha1>::new_from_slice(&self.secret)
            .expect("validated TOTP secrets are valid HMAC keys");
        mac.update(&counter.to_be_bytes());
        let digest = mac.finalize().into_bytes();
        let offset = usize::from(digest[digest.len() - 1] & 0x0f);
        let binary = (u32::from(digest[offset] & 0x7f) << 24)
            | (u32::from(digest[offset + 1]) << 16)
            | (u32::from(digest[offset + 2]) << 8)
            | u32::from(digest[offset + 3]);
        six_digit_code(binary % 1_000_000)
    }
}

impl LoginThrottle {
    fn prune(&mut self, now: u64) {
        let cutoff = now.saturating_sub(LOGIN_FAILURE_WINDOW_SECONDS);
        while self
            .failures
            .front()
            .is_some_and(|failure| *failure <= cutoff)
        {
            self.failures.pop_front();
        }
    }
}

fn normalize_totp_candidate(candidate: &str) -> ([u8; 6], bool) {
    let bytes = candidate.as_bytes();
    let mut normalized = [b'0'; 6];
    let valid = bytes.len() == normalized.len() && bytes.iter().all(u8::is_ascii_digit);
    if valid {
        normalized.copy_from_slice(bytes);
    }
    (normalized, valid)
}

fn six_digit_code(mut value: u32) -> [u8; 6] {
    let mut output = [b'0'; 6];
    for digit in output.iter_mut().rev() {
        *digit += u8::try_from(value % 10).expect("a decimal digit fits in u8");
        value /= 10;
    }
    output
}

fn session_digest_from_headers(headers: &HeaderMap) -> Option<[u8; 32]> {
    let mut cookie_headers = headers.get_all(COOKIE).iter();
    let cookie_header = cookie_headers.next()?;
    if cookie_headers.next().is_some() {
        return None;
    }
    let cookie_header = cookie_header.to_str().ok()?;
    if cookie_header.len() > MAX_COOKIE_HEADER_BYTES {
        return None;
    }

    let mut token = None;
    for cookie in cookie_header.split(';') {
        let Some((name, value)) = cookie.trim().split_once('=') else {
            continue;
        };
        if name.trim() != SESSION_COOKIE_NAME {
            continue;
        }
        if token.replace(value.trim()).is_some() {
            return None;
        }
    }
    let token = token?;
    if token.len() != 43 || !token.bytes().all(is_url_safe_token_byte) {
        return None;
    }
    let decoded = Zeroizing::new(URL_SAFE_NO_PAD.decode(token.as_bytes()).ok()?);
    if decoded.len() != SESSION_TOKEN_BYTES {
        return None;
    }
    Some(Sha256::digest(&*decoded).into())
}

fn is_url_safe_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    pub(crate) const USERNAME: &str = "admin";
    pub(crate) const PASSWORD: &str = "correct horse battery staple";
    pub(crate) const TOTP_SECRET: &str = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ";

    struct FixedClock(u64);

    impl Clock for FixedClock {
        fn now_seconds(&self) -> Result<u64, WebAuthUnavailable> {
            Ok(self.0)
        }
    }

    struct CountingTokenSource(AtomicU64);

    impl SessionTokenSource for CountingTokenSource {
        fn fill(&self, output: &mut [u8]) -> Result<(), WebAuthUnavailable> {
            let counter = self.0.fetch_add(1, Ordering::SeqCst).to_be_bytes();
            for (index, byte) in output.iter_mut().enumerate() {
                *byte = counter[index % counter.len()] ^ u8::try_from(index).unwrap_or_default();
            }
            Ok(())
        }
    }

    pub(crate) fn configured_service(now: u64, cookie_secure: bool) -> (WebAuthService, String) {
        let salt = "fixed-test-salt";
        let iterations = MIN_PBKDF2_ITERATIONS;
        let mut digest = [0_u8; PASSWORD_DIGEST_BYTES];
        pbkdf2_hmac::<Sha256>(
            PASSWORD.as_bytes(),
            salt.as_bytes(),
            iterations,
            &mut digest,
        );
        let password_hash = format!(
            "{PASSWORD_HASH_SCHEME}${iterations}${salt}${}",
            URL_SAFE_NO_PAD.encode(digest)
        );
        let service = WebAuthService::from_configuration_with_dependencies(
            WebAuthConfiguration {
                required: true,
                username: USERNAME.to_owned(),
                password_hash: Zeroizing::new(password_hash),
                totp_secret: Zeroizing::new(TOTP_SECRET.to_owned()),
                cookie_secure,
            },
            Arc::new(FixedClock(now)),
            Arc::new(CountingTokenSource(AtomicU64::new(1))),
        )
        .unwrap();
        let verifier = TotpVerifier::parse(TOTP_SECRET).unwrap();
        let code = String::from_utf8(verifier.code(now / 30).to_vec()).unwrap();
        (service, code)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::web_auth::test_support::{PASSWORD, TOTP_SECRET, USERNAME, configured_service};
    use axum::http::{HeaderValue, header::COOKIE};

    const PASSWORD_VECTOR: &str =
        "pbkdf2_sha256$260000$fixed-test-salt$0SXm18RXYyJ7n1d3OrNHm0BljM50P_ECd9D5TIrptig";

    struct MutableClock(Arc<AtomicU64>);

    impl Clock for MutableClock {
        fn now_seconds(&self) -> Result<u64, WebAuthUnavailable> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    fn session_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!("{SESSION_COOKIE_NAME}={token}")).unwrap(),
        );
        headers
    }

    #[test]
    fn legacy_python_password_hash_vector_is_verified() {
        let verifier = PasswordVerifier::parse(PASSWORD_VECTOR).unwrap();

        assert!(verifier.verify(PASSWORD));
        assert!(!verifier.verify("incorrect password"));
    }

    #[test]
    fn password_hash_parser_rejects_unbounded_work_and_malformed_values() {
        for value in [
            "pbkdf2_sha256$999999999$fixed-test-salt$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "pbkdf2_sha1$260000$fixed-test-salt$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "pbkdf2_sha256$260000$short$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "pbkdf2_sha256$260000$fixed-test-salt$bad+base64",
        ] {
            assert_eq!(
                PasswordVerifier::parse(value).err(),
                Some(WebAuthConfigurationError::InvalidPasswordHash)
            );
        }
    }

    #[test]
    fn totp_matches_rfc_sha1_vector_and_accepts_only_the_adjacent_window() {
        let verifier = TotpVerifier::parse(TOTP_SECRET).unwrap();

        assert!(verifier.verify_at("287082", 59));
        assert!(verifier.verify_at("287082", 89));
        assert!(!verifier.verify_at("287082", 119));
        assert!(!verifier.verify_at("28 7082", 59));
        assert!(!verifier.verify_at("2870827", 59));
    }

    #[test]
    fn partial_required_configuration_fails_closed_without_exposing_secrets() {
        let password_hash = "pbkdf2_sha256$260000$fixed-test-salt$secret-value";
        let configuration = WebAuthConfiguration {
            required: true,
            username: USERNAME.to_owned(),
            password_hash: Zeroizing::new(password_hash.to_owned()),
            totp_secret: Zeroizing::new(String::new()),
            cookie_secure: true,
        };
        let debug = format!("{configuration:?}");
        let service = WebAuthService::from_configuration(configuration).unwrap();

        assert!(!debug.contains(password_hash));
        assert_eq!(
            service.status(&HeaderMap::new()).unwrap(),
            WebAuthStatus {
                required: true,
                configured: false,
                authenticated: false,
                username: None,
            }
        );
        assert_eq!(
            service.authorize(&HeaderMap::new()).unwrap_err(),
            WebAuthorizationError::NotConfigured
        );
    }

    #[test]
    fn successful_login_creates_an_opaque_digest_only_session() {
        let (service, code) = configured_service(59, true);
        let outcome = service.login(USERNAME, PASSWORD, &code).unwrap();
        let WebLoginOutcome::Authenticated { session_token } = outcome else {
            panic!("required authentication must create a session")
        };
        let headers = session_headers(&session_token);

        assert_eq!(session_token.len(), 43);
        assert_eq!(
            service.status(&headers).unwrap(),
            WebAuthStatus {
                required: true,
                configured: true,
                authenticated: true,
                username: Some(USERNAME.to_owned()),
            }
        );
        let debug = format!("{service:?}");
        assert!(!debug.contains(&*session_token));
        assert!(!debug.contains(TOTP_SECRET));
        assert!(!debug.contains(PASSWORD));
        let sessions = service.inner.sessions.lock().unwrap();
        assert_eq!(sessions.len(), 1);
        assert!(!hex::encode(sessions.keys().next().unwrap()).contains(&*session_token));
    }

    #[test]
    fn duplicate_and_oversized_cookie_headers_never_authenticate() {
        let (service, code) = configured_service(59, false);
        let WebLoginOutcome::Authenticated { session_token } =
            service.login(USERNAME, PASSWORD, &code).unwrap()
        else {
            unreachable!()
        };

        let mut duplicate = HeaderMap::new();
        duplicate.append(
            COOKIE,
            HeaderValue::from_str(&format!("{SESSION_COOKIE_NAME}={}", session_token.as_str()))
                .unwrap(),
        );
        duplicate.append(
            COOKIE,
            HeaderValue::from_str(&format!("{SESSION_COOKIE_NAME}={}", session_token.as_str()))
                .unwrap(),
        );
        assert_eq!(
            service.authorize(&duplicate).unwrap_err(),
            WebAuthorizationError::NotAuthenticated
        );

        let mut repeated = HeaderMap::new();
        repeated.insert(
            COOKIE,
            HeaderValue::from_str(&format!(
                "{SESSION_COOKIE_NAME}={}; {SESSION_COOKIE_NAME}={}",
                session_token.as_str(),
                session_token.as_str()
            ))
            .unwrap(),
        );
        assert_eq!(
            service.authorize(&repeated).unwrap_err(),
            WebAuthorizationError::NotAuthenticated
        );

        let mut oversized = HeaderMap::new();
        oversized.insert(
            COOKIE,
            HeaderValue::from_str(&format!(
                "noise={}",
                "x".repeat(MAX_COOKIE_HEADER_BYTES + 1)
            ))
            .unwrap(),
        );
        assert_eq!(
            service.authorize(&oversized).unwrap_err(),
            WebAuthorizationError::NotAuthenticated
        );
    }

    #[test]
    fn logout_revokes_the_server_side_session() {
        let (service, code) = configured_service(59, false);
        let WebLoginOutcome::Authenticated { session_token } =
            service.login(USERNAME, PASSWORD, &code).unwrap()
        else {
            unreachable!()
        };
        let headers = session_headers(&session_token);

        service.logout(&headers).unwrap();

        assert_eq!(
            service.authorize(&headers).unwrap_err(),
            WebAuthorizationError::NotAuthenticated
        );
        assert!(service.inner.sessions.lock().unwrap().is_empty());
    }

    #[test]
    fn session_expires_at_the_exact_server_side_deadline() {
        let now = Arc::new(AtomicU64::new(59));
        let service = WebAuthService::from_configuration_with_dependencies(
            WebAuthConfiguration {
                required: true,
                username: USERNAME.to_owned(),
                password_hash: Zeroizing::new(PASSWORD_VECTOR.to_owned()),
                totp_secret: Zeroizing::new(TOTP_SECRET.to_owned()),
                cookie_secure: false,
            },
            Arc::new(MutableClock(Arc::clone(&now))),
            Arc::new(OsSessionTokenSource),
        )
        .unwrap();
        let WebLoginOutcome::Authenticated { session_token } =
            service.login(USERNAME, PASSWORD, "287082").unwrap()
        else {
            unreachable!()
        };
        let headers = session_headers(&session_token);

        now.store(59 + SESSION_TTL_SECONDS - 1, Ordering::SeqCst);
        assert!(service.status(&headers).unwrap().authenticated);

        now.store(59 + SESSION_TTL_SECONDS, Ordering::SeqCst);
        assert!(!service.status(&headers).unwrap().authenticated);
        assert!(service.inner.sessions.lock().unwrap().is_empty());
    }

    #[test]
    fn repeated_bad_logins_are_bounded_before_more_password_work() {
        let (service, code) = configured_service(59, false);
        for _ in 0..MAX_LOGIN_FAILURES_PER_WINDOW {
            assert_eq!(
                service.login(USERNAME, "wrong", &code).err(),
                Some(WebLoginError::InvalidCredentials)
            );
        }
        assert_eq!(
            service.login(USERNAME, PASSWORD, &code).err(),
            Some(WebLoginError::RateLimited)
        );
    }
}
