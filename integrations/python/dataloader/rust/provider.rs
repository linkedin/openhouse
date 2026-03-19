use std::any::Any;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::pyarrow::FromPyArrow;
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider, TableType};
use datafusion_common::Result as DFResult;
use datafusion_expr::{Expr, TableProviderFilterPushDown};
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_physical_plan::ExecutionPlan;
use pyo3::exceptions::PyValueError;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::exec::PythonExec;

// ---------------------------------------------------------------------------
// PyCapsule utilities (adapted from datafusion-ffi-example)
// ---------------------------------------------------------------------------

fn validate_pycapsule(capsule: &Bound<PyCapsule>, name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if capsule_name.is_none() {
        return Err(PyValueError::new_err(format!(
            "Expected {name} PyCapsule to have name set."
        )));
    }
    let capsule_name = unsafe { capsule_name.unwrap().as_cstr().to_str()? };
    if capsule_name != name {
        return Err(PyValueError::new_err(format!(
            "Expected name '{name}' in PyCapsule, instead got '{capsule_name}'"
        )));
    }
    Ok(())
}

fn ffi_logical_codec_from_pycapsule(obj: Bound<PyAny>) -> PyResult<FFI_LogicalExtensionCodec> {
    let attr_name = "__datafusion_logical_extension_codec__";
    let capsule = if obj.hasattr(attr_name)? {
        obj.getattr(attr_name)?.call0()?
    } else {
        obj
    };
    let capsule = capsule.downcast::<PyCapsule>()?;
    validate_pycapsule(capsule, "datafusion_logical_extension_codec")?;
    let data: NonNull<FFI_LogicalExtensionCodec> = capsule
        .pointer_checked(Some(c_str!("datafusion_logical_extension_codec")))?
        .cast();
    let codec = unsafe { data.as_ref() };
    Ok(codec.clone())
}

// ---------------------------------------------------------------------------
// PythonTableProvider — exposed to Python via #[pyclass]
// ---------------------------------------------------------------------------

/// Wraps a Python scan delegate as a DataFusion TableProvider.
///
/// The delegate must implement:
///   - `schema() -> pyarrow.Schema`
///   - `scan(projection: list[int] | None, filters: list[str], limit: int | None)
///          -> pyarrow.RecordBatchReader`
#[pyclass(name = "PythonTableProvider", module = "openhouse.dataloader._native")]
pub(crate) struct PythonTableProvider {
    delegate: Py<PyAny>,
    schema: SchemaRef,
}

#[pymethods]
impl PythonTableProvider {
    #[new]
    fn new(delegate: Py<PyAny>) -> PyResult<Self> {
        let schema = Python::attach(|py| {
            let obj = delegate.bind(py);
            let schema_obj = obj.call_method0("schema")?;
            let schema = Schema::from_pyarrow_bound(&schema_obj)?;
            Ok::<_, PyErr>(Arc::new(schema))
        })?;
        Ok(Self { delegate, schema })
    }

    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let inner = InnerProvider {
            delegate: self.delegate.clone_ref(py),
            schema: self.schema.clone(),
        };
        let name = cr"datafusion_table_provider".into();
        let codec = ffi_logical_codec_from_pycapsule(session)?;
        let provider =
            FFI_TableProvider::new_with_ffi_codec(Arc::new(inner), true, None, codec);
        PyCapsule::new(py, provider, Some(name))
    }
}

// ---------------------------------------------------------------------------
// InnerProvider — implements DataFusion's TableProvider trait
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct InnerProvider {
    delegate: Py<PyAny>,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for InnerProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let filter_strings: Vec<String> = filters.iter().map(|f| f.to_string()).collect();

        let projected_schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => self.schema.clone(),
        };

        Ok(Arc::new(PythonExec::new(
            self.delegate.clone(),
            projected_schema,
            projection.cloned(),
            filter_strings,
            limit,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Return Inexact: DataFusion also applies post-scan filters for correctness.
        // The delegate may use these hints for performance (e.g. partition pruning).
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
