//! Engine-owned NIF implementations for the `aion_flow_ffi` namespace.
//!
//! These NIFs back the `@external(erlang, "aion_flow_ffi", ...)` declarations
//! in the Gleam `aion_flow` SDK. `run_activity` is registered as a dirty NIF
//! because activity dispatch may block on network I/O.

use std::cell::RefCell;

use beamr::atom::Atom;
use beamr::native::ProcessContext;
use beamr::term::binary::{self, Binary};
use beamr::term::boxed;
use beamr::term::Term;

use crate::activity::bridge::activity_dispatcher;

use super::nif::{Mfa, NifEntry};

const FFI_MODULE: &str = "aion_flow_ffi";

thread_local! {
    static NIF_HEAP: RefCell<Vec<Box<[u64]>>> = const { RefCell::new(Vec::new()) };
}

fn park_heap(heap: Box<[u64]>) {
    NIF_HEAP.with_borrow_mut(|parked| parked.push(heap));
}

#[cfg(test)]
fn clear_parked_heap() {
    NIF_HEAP.with_borrow_mut(Vec::clear);
}

fn alloc_binary_term(bytes: &[u8]) -> Option<Term> {
    let word_count = 2 + binary::packed_word_count(bytes.len());
    let mut heap = vec![0_u64; word_count].into_boxed_slice();
    let term = binary::write_binary(&mut heap, bytes)?;
    park_heap(heap);
    Some(term)
}

fn alloc_tuple_term(elements: &[Term]) -> Option<Term> {
    let word_count = 1 + elements.len();
    let mut heap = vec![0_u64; word_count].into_boxed_slice();
    let term = boxed::write_tuple(&mut heap, elements)?;
    park_heap(heap);
    Some(term)
}

fn ok_result_term(value: &str) -> Option<Term> {
    let value_term = alloc_binary_term(value.as_bytes())?;
    alloc_tuple_term(&[Term::atom(Atom::OK), value_term])
}

fn error_result_term(message: &str) -> Option<Term> {
    let value_term = alloc_binary_term(message.as_bytes())?;
    alloc_tuple_term(&[Term::atom(Atom::ERROR), value_term])
}

fn decode_string_arg(term: Term) -> Result<String, String> {
    let bin = Binary::new(term).ok_or_else(|| "argument is not a binary".to_owned())?;
    String::from_utf8(bin.as_bytes().to_vec()).map_err(|_| "argument is not valid UTF-8".to_owned())
}

/// NIF backing `aion_flow_ffi:run_activity/3`.
///
/// Heap from the previous NIF invocation is drained first (matching
/// `NifContext`'s one-call retention window), then fresh allocations for
/// the return value are parked for the scheduler to copy.
fn run_activity(args: &[Term], _ctx: &mut ProcessContext) -> Result<Term, Term> {
    if args.len() > 255 {
        return Err(Term::NIL);
    }

    if args.len() != 3 {
        let msg = format!("run_activity: expected 3 arguments, got {}", args.len());
        return Ok(error_result_term(&msg).unwrap_or(Term::NIL));
    }

    let name = match decode_string_arg(args[0]) {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("run_activity name: {e}");
            return Ok(error_result_term(&msg).unwrap_or(Term::NIL));
        }
    };
    let input = match decode_string_arg(args[1]) {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("run_activity input: {e}");
            return Ok(error_result_term(&msg).unwrap_or(Term::NIL));
        }
    };
    let config = match decode_string_arg(args[2]) {
        Ok(s) => s,
        Err(e) => {
            let msg = format!("run_activity config: {e}");
            return Ok(error_result_term(&msg).unwrap_or(Term::NIL));
        }
    };

    let Some(dispatcher) = activity_dispatcher() else {
        return Ok(error_result_term(
            "no activity dispatcher configured — \
             set one via EngineBuilder::activity_dispatcher",
        )
        .unwrap_or(Term::NIL));
    };

    match dispatcher.dispatch(&name, &input, &config) {
        Ok(result) => Ok(ok_result_term(&result).unwrap_or(Term::NIL)),
        Err(error) => Ok(error_result_term(&error).unwrap_or(Term::NIL)),
    }
}

/// Collect engine-owned NIF entries for `aion_flow_ffi`.
pub(super) fn engine_nif_entries() -> Vec<NifEntry> {
    vec![NifEntry::dirty(
        Mfa::new(FFI_MODULE, "run_activity", 3),
        run_activity,
    )]
}

#[cfg(test)]
mod tests {
    use beamr::native::ProcessContext;
    use beamr::term::binary::Binary;
    use beamr::term::boxed::Tuple;
    use beamr::term::Term;

    use super::{alloc_binary_term, clear_parked_heap, engine_nif_entries, run_activity};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn binary_arg(value: &str) -> Term {
        alloc_binary_term(value.as_bytes()).unwrap_or(Term::NIL)
    }

    fn decode_result_tuple(term: Term) -> Result<(String, String), Box<dyn std::error::Error>> {
        let tuple = Tuple::new(term).ok_or("result should be a tuple")?;
        if tuple.arity() != 2 {
            return Err(format!("expected arity 2, got {}", tuple.arity()).into());
        }
        let tag = tuple.get(0).ok_or("missing tag element")?;
        let value = tuple.get(1).ok_or("missing value element")?;
        let tag_name = if tag == Term::atom(beamr::atom::Atom::OK) {
            "ok"
        } else {
            "error"
        };
        let bin = Binary::new(value).ok_or("value should be a binary")?;
        let text = String::from_utf8(bin.as_bytes().to_vec())
            .map_err(|_| "value should be valid UTF-8")?;
        Ok((tag_name.to_owned(), text))
    }

    #[test]
    fn engine_nif_entries_match_expected_registrations() {
        let expected = [("aion_flow_ffi", "run_activity", 3_u8)];
        let actual = engine_nif_entries();

        for (expected_module, expected_function, expected_arity) in expected {
            let matching_function = actual.iter().find(|entry| {
                entry.mfa.module == expected_module && entry.mfa.function == expected_function
            });
            let Some(entry) = matching_function else {
                panic!(
                    "missing expected NIF {expected_module}:{expected_function}/{expected_arity}"
                );
            };

            assert_eq!(
                entry.mfa.arity, expected_arity,
                "wrong arity for NIF {expected_module}:{expected_function}: expected {expected_arity}, got {}",
                entry.mfa.arity
            );
        }

        assert_eq!(
            actual.len(),
            expected.len(),
            "registered engine NIF count differs from expected list: expected {}, got {}. actual registrations: {}",
            expected.len(),
            actual.len(),
            actual
                .iter()
                .map(|entry| entry.mfa.display())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    #[test]
    fn returns_result_tuple_for_valid_call() -> TestResult {
        use std::sync::Arc;

        use crate::activity::bridge::{install_activity_dispatcher, ActivityDispatcher};

        struct TestDispatcher;
        impl ActivityDispatcher for TestDispatcher {
            fn dispatch(&self, _name: &str, input: &str, _config: &str) -> Result<String, String> {
                Ok(format!("dispatched:{input}"))
            }
        }
        install_activity_dispatcher(Arc::new(TestDispatcher));

        clear_parked_heap();
        let name = binary_arg("greet");
        let input = binary_arg("{\"name\":\"Alice\"}");
        let config = binary_arg("{}");
        let mut ctx = ProcessContext::new();

        let result = run_activity(&[name, input, config], &mut ctx);

        match result {
            Ok(term) => {
                let (tag, _value) = decode_result_tuple(term)?;
                assert!(
                    tag == "ok" || tag == "error",
                    "result should be a tagged tuple"
                );
            }
            Err(_) => return Err("NIF should return Ok at the beamr level".into()),
        }
        Ok(())
    }

    #[test]
    fn returns_error_on_wrong_arity() -> TestResult {
        clear_parked_heap();
        let mut ctx = ProcessContext::new();

        let result = run_activity(&[], &mut ctx);

        match result {
            Ok(term) => {
                let (tag, message) = decode_result_tuple(term)?;
                assert_eq!(tag, "error");
                assert!(
                    message.contains("expected 3 arguments"),
                    "unexpected: {message}"
                );
            }
            Err(_) => return Err("NIF should return Ok at the beamr level".into()),
        }
        Ok(())
    }
}
