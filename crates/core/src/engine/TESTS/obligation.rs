use std::panic;

use super::{Obligation, Transfer, Work};

struct PendingWork(bool);

impl Work for PendingWork {
    fn label(&self) -> &'static str {
        "transferred work"
    }

    fn is_pending(&self) -> bool {
        self.0
    }
}

struct WorkTransition {
    pending: bool,
    next: PendingWork,
}

impl Work for WorkTransition {
    fn label(&self) -> &'static str {
        "original work"
    }

    fn is_pending(&self) -> bool {
        self.pending
    }
}

impl Transfer for WorkTransition {
    type Target = PendingWork;

    fn transfer(self) -> Self::Target {
        self.next
    }
}

#[test]
fn transfer_guards_pending_work_and_preserves_its_origin() {
    for (source_pending, target_pending, expected_label) in [
        (false, false, None),
        (false, true, Some("transferred work")),
        (true, false, Some("original work")),
        (true, true, Some("original work")),
    ] {
        let result = panic::catch_unwind(|| {
            let obligation = Obligation::new(WorkTransition {
                pending: source_pending,
                next: PendingWork(target_pending),
            });
            drop(obligation.transfer());
        });
        if let Some(label) = expected_label {
            let panic = result.expect_err("transferred work must remain guarded");
            assert!(panic.downcast_ref::<String>().is_some_and(|message| {
                message == &format!("unfinished lifecycle obligation: {label}")
            }));
        } else {
            assert!(result.is_ok(), "empty transfers must remain harmless");
        }
    }
}
