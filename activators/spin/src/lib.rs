#[allow(warnings)]
mod bindings;

use bindings::*;

struct Component;

export!(Component);

impl Guest for Component {
    fn process_events(_events: Vec<Event>) -> Vec<Action> {
        #[allow(clippy::empty_loop)]
        loop {}
    }
}
