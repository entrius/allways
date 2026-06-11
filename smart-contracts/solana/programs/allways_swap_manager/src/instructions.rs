pub mod admin;
pub mod cancel_reservation;
pub mod confirm_swap;
pub mod deactivate;
pub mod initialize;
pub mod mark_fulfilled;
pub mod post_collateral;
pub mod timeout_swap;
pub mod vote_activate;
pub mod vote_deactivate;
pub mod vote_initiate;
pub mod vote_reserve;
pub mod withdraw_collateral;
pub mod withdraw_treasury;

// Glob re-exports so the `#[program]` macro sees each instruction's Accounts struct.
// (Emits a benign "ambiguous glob re-exports" warning for the shared `handler` name;
// handlers are always called fully-qualified, e.g. `initialize::handler`.)
pub use admin::*;
pub use cancel_reservation::*;
pub use confirm_swap::*;
pub use deactivate::*;
pub use initialize::*;
pub use mark_fulfilled::*;
pub use post_collateral::*;
pub use timeout_swap::*;
pub use vote_activate::*;
pub use vote_deactivate::*;
pub use vote_initiate::*;
pub use vote_reserve::*;
pub use withdraw_collateral::*;
pub use withdraw_treasury::*;
