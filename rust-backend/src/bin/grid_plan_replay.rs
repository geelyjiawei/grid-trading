use std::{env, fs, path::Path};

use anyhow::{Context, bail};
use grid_trading_server::{
    domain::{GridConfig, InstrumentRules, OrderKind, OrderSide, TimeInForce},
    engine::{GridOrderRole, GridPlan, MarketSnapshot, build_grid_plan},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct ReplayFixture {
    version: u8,
    cases: Vec<ReplayCase>,
}

#[derive(Deserialize)]
struct ReplayCase {
    name: String,
    config: GridConfig,
    market: MarketSnapshot,
    rules: InstrumentRules,
}

#[derive(Serialize)]
struct ReplayOutput {
    version: u8,
    results: Vec<ReplayResult>,
}

#[derive(Serialize)]
struct ReplayResult {
    name: String,
    plan: NormalizedPlan,
}

#[derive(Serialize)]
struct NormalizedPlan {
    reference_price: String,
    levels: Vec<String>,
    active_grid_count: u16,
    participating_level_count: u16,
    total_quantity: String,
    opening_order: Option<NormalizedOpeningOrder>,
    grid_orders: Vec<NormalizedGridOrder>,
}

#[derive(Serialize)]
struct NormalizedOpeningOrder {
    side: &'static str,
    price: Option<String>,
    quantity: String,
    kind: &'static str,
    time_in_force: &'static str,
}

#[derive(Serialize)]
struct NormalizedGridOrder {
    level_index: u16,
    side: &'static str,
    price: String,
    quantity: String,
    reduce_only: bool,
    time_in_force: &'static str,
    role: &'static str,
}

fn main() -> anyhow::Result<()> {
    let mut arguments = env::args_os();
    let _program = arguments.next();
    let Some(path) = arguments.next() else {
        bail!("usage: grid-plan-replay <fixture.json>");
    };
    if arguments.next().is_some() {
        bail!("usage: grid-plan-replay <fixture.json>");
    }

    let fixture = load_fixture(Path::new(&path))?;
    if fixture.version != 1 {
        bail!("unsupported replay fixture version {}", fixture.version);
    }
    let mut results = Vec::with_capacity(fixture.cases.len());
    for case in fixture.cases {
        let plan = build_grid_plan(&case.config, &case.market, &case.rules)
            .with_context(|| format!("grid replay case {} failed", case.name))?;
        results.push(ReplayResult {
            name: case.name,
            plan: normalize_plan(plan),
        });
    }
    let output = ReplayOutput {
        version: 1,
        results,
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn load_fixture(path: &Path) -> anyhow::Result<ReplayFixture> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read replay fixture {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("invalid replay fixture {}", path.display()))
}

fn normalize_plan(plan: GridPlan) -> NormalizedPlan {
    NormalizedPlan {
        reference_price: decimal_text(plan.reference_price),
        levels: plan.levels.into_iter().map(decimal_text).collect(),
        active_grid_count: plan.active_grid_count,
        participating_level_count: plan.participating_level_count,
        total_quantity: decimal_text(plan.total_quantity),
        opening_order: plan.opening_order.map(|order| NormalizedOpeningOrder {
            side: side_text(order.side),
            price: order.price.map(decimal_text),
            quantity: decimal_text(order.quantity),
            kind: kind_text(order.kind),
            time_in_force: time_in_force_text(order.time_in_force),
        }),
        grid_orders: plan
            .grid_orders
            .into_iter()
            .map(|order| NormalizedGridOrder {
                level_index: order.level_index,
                side: side_text(order.side),
                price: decimal_text(order.price),
                quantity: decimal_text(order.quantity),
                reduce_only: order.reduce_only,
                time_in_force: time_in_force_text(order.time_in_force),
                role: match order.role {
                    GridOrderRole::Profit => "profit",
                    GridOrderRole::Add => "add",
                },
            })
            .collect(),
    }
}

fn decimal_text(value: Decimal) -> String {
    value.normalize().to_string()
}

fn side_text(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

fn kind_text(kind: OrderKind) -> &'static str {
    match kind {
        OrderKind::Limit => "limit",
        OrderKind::Market => "market",
    }
}

fn time_in_force_text(time_in_force: TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::Gtc => "gtc",
        TimeInForce::PostOnly => "post_only",
    }
}
