package harness

/**
 * The branch capability: everything a table's named references decide, integrated as one contribution.
 *
 * A named reference binds a name to one snapshot, and every behavior below follows from that binding. The lifecycle
 * family owns the bindings themselves; the write family owns which effects a write aimed at a branch keeps to the
 * branch and which ones the table shares; the merge family owns the operations that move one binding onto another;
 * the intersection family owns the branch's side of time travel, rename, maintenance, table evolution and the
 * declared file format; and the merge-on-read family owns the position-delete file a branch mutation records on a
 * table whose mutations are written that way.
 *
 * Each family lives in its own file with its own operations, preparation axes and count, so a reviewer reads one
 * contract at a time. This trait names them in the order the contribution integrates them, and
 * `ScenarioBranchKit` holds the preparations and lookups they share.
 *
 * Case families: 34 families over 2 columnar formats, contributing 68 cases: 16 lifecycle, 18 write, 10 merge, 14
 * intersection and 10 merge-on-read.
 */
trait ScenarioBranch
    extends ScenarioBranchLifecycle
    with ScenarioBranchWrite
    with ScenarioBranchMerge
    with ScenarioBranchIntersection
    with ScenarioBranchMergeOnRead {

  /** Every branch case, in the order this capability integrates its contract families. */
  lazy val branchCases: List[TestCase] =
    branchLifecycleCases ++
      branchWriteCases ++
      branchMergeCases ++
      branchIntersectionCases ++
      branchMergeOnReadCases

}
