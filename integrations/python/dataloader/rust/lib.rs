mod exec;
mod provider;

use pyo3::prelude::*;

use provider::PythonTableProvider;

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PythonTableProvider>()?;
    Ok(())
}
