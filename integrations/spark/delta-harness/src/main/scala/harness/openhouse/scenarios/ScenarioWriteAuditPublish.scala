package harness

/**
 * The write-audit-publish capability: what the table property means, and what staging and publishing a write do,
 * integrated as one contribution.
 *
 * The configuration family owns the property and the two session settings that read it, including the states the
 * catalog rejects. The staging family owns what a write made under an identifier commits, what `main` reads while it
 * is staged, and what each publish does to `main`. Each family lives in its own file with its own operations,
 * preparation axes and count, and `ScenarioBranchKit` holds the preparations and lookups they share with the branch
 * capability.
 *
 * Case families: 17 families over 2 columnar formats, contributing 34 cases: 14 configuration and 20 staging and
 * publish.
 */
trait ScenarioWriteAuditPublish
    extends ScenarioWriteAuditPublishConfiguration
    with ScenarioWriteAuditPublishStaging {

  /** Every write-audit-publish case: the configuration contract, then staging and publish. */
  lazy val writeAuditPublishCases: List[TestCase] =
    writeAuditPublishConfigurationCases ++ writeAuditPublishStagingCases

}
