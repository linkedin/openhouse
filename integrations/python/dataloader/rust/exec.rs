use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::pyarrow::PyArrowType;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream, Statistics,
};
use futures::stream;
use futures::TryStreamExt;
use pyo3::prelude::*;
use pyo3::types::PyIterator;

// ---------------------------------------------------------------------------
// PyArrow batch adapter (follows datafusion-python's DatasetExec pattern)
// ---------------------------------------------------------------------------

struct PyArrowBatchesAdapter {
    batches: Py<PyIterator>,
}

impl Iterator for PyArrowBatchesAdapter {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Python::attach(|py| {
            let batches = self.batches.clone_ref(py).into_bound(py);
            Some(
                batches
                    .next()?
                    .and_then(|batch| Ok(batch.extract::<PyArrowType<RecordBatch>>()?.0))
                    .map_err(|err| ArrowError::ExternalError(Box::new(err))),
            )
        })
    }
}

// ---------------------------------------------------------------------------
// PythonExec — DataFusion ExecutionPlan backed by a Python scan delegate
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct PythonExec {
    delegate: Py<PyAny>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filter_strings: Vec<String>,
    limit: Option<usize>,
    plan_properties: Arc<PlanProperties>,
}

impl PythonExec {
    pub fn new(
        delegate: Py<PyAny>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filter_strings: Vec<String>,
        limit: Option<usize>,
    ) -> Self {
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            delegate,
            schema,
            projection,
            filter_strings,
            limit,
            plan_properties,
        }
    }
}

impl ExecutionPlan for PythonExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion_physical_plan::execution::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let schema = self.schema.clone();

        Python::attach(|py| {
            let delegate = self.delegate.bind(py);

            // Convert projection to Option<list[int]> or None
            let py_projection: PyObject = match &self.projection {
                Some(p) => pyo3::types::PyList::new(py, p.iter().copied())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .into_any()
                    .unbind(),
                None => py.None().into(),
            };

            let py_filters = pyo3::types::PyList::new(py, &self.filter_strings)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Convert limit to int or None
            let py_limit: PyObject = match self.limit {
                Some(l) => (l as u64)
                    .into_pyobject(py)
                    .map_err(|e| DataFusionError::External(Box::new(e.into())))?
                    .into_any()
                    .unbind(),
                None => py.None().into(),
            };

            // Call delegate.scan(projection, filters, limit)
            let reader_obj = delegate
                .call_method1(
                    "scan",
                    (
                        py_projection.bind(py),
                        py_filters.as_any(),
                        py_limit.bind(py),
                    ),
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Wrap the Python iterator as a RecordBatchStream
            let batches: Bound<'_, PyIterator> = reader_obj
                .try_iter()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let adapter = PyArrowBatchesAdapter {
                batches: batches.unbind(),
            };

            let record_batch_stream = stream::iter(adapter);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                record_batch_stream.map_err(|e| e.into()),
            )) as SendableRecordBatchStream)
        })
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }
}

impl ExecutionPlanProperties for PythonExec {
    fn output_partitioning(&self) -> &Partitioning {
        self.plan_properties.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        None
    }

    fn boundedness(&self) -> Boundedness {
        self.plan_properties.boundedness
    }

    fn pipeline_behavior(&self) -> EmissionType {
        self.plan_properties.emission_type
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        &self.plan_properties.eq_properties
    }
}

impl DisplayAs for PythonExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                let columns: Vec<&str> = self
                    .schema
                    .fields()
                    .iter()
                    .map(|x| x.name().as_str())
                    .collect();
                write!(
                    f,
                    "PythonExec: projection=[{}], filters={:?}, limit={:?}",
                    columns.join(", "),
                    self.filter_strings,
                    self.limit,
                )
            }
        }
    }
}
