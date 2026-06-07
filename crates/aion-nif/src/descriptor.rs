//! NIF descriptor, `Determinism` tag, and dirty flag.

use beamr::native::NativeFn;

/// Classifies how the engine is allowed to bind and execute a NIF.
///
/// The engine registration path reads this tag to decide binding mode:
/// [`Determinism::Pure`] functions may be bound inline and re-executed during
/// replay, while [`Determinism::SideEffectful`] functions are activity bodies
/// only and are never bound inline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Determinism {
    /// Deterministic helper that may be called inline from workflow code.
    Pure,
    /// Side-effectful native activity that must be invoked through the activity contract.
    SideEffectful,
}

/// Descriptor emitted by `aion-nif` for the engine runtime to register.
#[derive(Clone, Debug)]
pub struct Nif {
    module: String,
    function: String,
    arity: u8,
    native: NativeFn,
    dirty: bool,
    determinism: Determinism,
}

impl Nif {
    /// Creates a descriptor for a native function declaration.
    #[must_use]
    pub(crate) fn new(
        module: impl Into<String>,
        function: impl Into<String>,
        arity: u8,
        native: NativeFn,
        dirty: bool,
        determinism: Determinism,
    ) -> Self {
        Self {
            module: module.into(),
            function: function.into(),
            arity,
            native,
            dirty,
            determinism,
        }
    }

    /// Creates a pure deterministic helper descriptor.
    #[must_use]
    pub fn pure(
        module: impl Into<String>,
        function: impl Into<String>,
        arity: u8,
        native: NativeFn,
    ) -> Self {
        Self::new(module, function, arity, native, false, Determinism::Pure)
    }

    /// Creates a side-effectful activity descriptor that should run dirty.
    #[must_use]
    pub fn side_effectful(
        module: impl Into<String>,
        function: impl Into<String>,
        arity: u8,
        native: NativeFn,
    ) -> Self {
        Self::new(
            module,
            function,
            arity,
            native,
            true,
            Determinism::SideEffectful,
        )
    }

    /// Gleam or Elixir module atom-name.
    #[must_use]
    pub fn module(&self) -> &str {
        &self.module
    }

    /// Gleam or Elixir function atom-name.
    #[must_use]
    pub fn function(&self) -> &str {
        &self.function
    }

    /// Declared function arity.
    #[must_use]
    pub const fn arity(&self) -> u8 {
        self.arity
    }

    /// Native shim invoked by the engine's beamr registration boundary.
    #[must_use]
    pub const fn native(&self) -> NativeFn {
        self.native
    }

    /// Whether the engine should register this NIF on beamr's dirty scheduler.
    #[must_use]
    pub const fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Determinism tag used by the engine to choose inline vs activity binding.
    #[must_use]
    pub const fn determinism(&self) -> Determinism {
        self.determinism
    }
}

#[cfg(test)]
mod tests {
    use beamr::{native::ProcessContext, term::Term};

    use super::{Determinism, Nif};

    fn identity_native(args: &[Term], ctx: &mut ProcessContext) -> Result<Term, Term> {
        let _ = ctx;
        args.first().copied().ok_or(Term::NIL)
    }

    #[test]
    fn descriptor_exposes_identity_dirty_flag_and_determinism() {
        let nif = Nif::pure("example/module", "identity", 1, identity_native);

        assert_eq!(nif.module(), "example/module");
        assert_eq!(nif.function(), "identity");
        assert_eq!(nif.arity(), 1);
        assert!(!nif.is_dirty());
        assert_eq!(nif.determinism(), Determinism::Pure);
        assert_eq!(
            nif.native() as *const () as usize,
            identity_native as *const () as usize
        );
    }
}
