grammar OpenhouseSqlExtensions;

@lexer::members {
  /**
   * This method will be called when we see '/*' and try to match it as a bracketed comment.
   * If the next character is '+', it should be parsed as hint later, and we cannot match
   * it as a bracketed comment.
   *
   * Returns true if the next character is '+'.
   */
  public boolean isHint() {
    int nextChar = _input.LA(1);
    if (nextChar == '+') {
      return true;
    } else {
      return false;
    }
  }
}

singleStatement
    : statement EOF
    ;

statement
  : ALTER TABLE multipartIdentifier SET POLICY '(' retentionPolicy (columnRetentionPolicy)? ')'        #setRetentionPolicy
  | ALTER TABLE multipartIdentifier SET POLICY '(' replicationPolicy ')'                               #setReplicationPolicy
  | ALTER TABLE multipartIdentifier UNSET POLICY '(' replication ')'                                   #unSetReplicationPolicy
  | ALTER TABLE multipartIdentifier SET POLICY '(' sharingPolicy ')'                                   #setSharingPolicy
  | ALTER TABLE multipartIdentifier SET POLICY '(' historyPolicy ')'                                   #setHistoryPolicy
  | ALTER TABLE multipartIdentifier MODIFY columnNameClause SET columnPolicy                           #setColumnPolicyTag
  | GRANT privilege ON grantableResource TO principal                                                  #grantStatement
  | REVOKE privilege ON grantableResource FROM principal                                               #revokeStatement
  | SHOW GRANTS ON grantableResource                                                                   #showGrantsStatement
  ;

multipartIdentifier
    : parts+=identifier ('.' parts+=identifier)*
    ;

privilege
    : columnLevelPrivilege
    | SELECT | DESCRIBE | ALTER | GRANT_REVOKE | CREATE_TABLE
    ;

columnLevelPrivilege
    : SELECT policyTag
    ;

grantableResource
    : TABLE multipartIdentifier
    | DATABASE multipartIdentifier
    ;

principal
    : identifier
    ;

identifier
    : IDENTIFIER
    | quotedIdentifier
    | nonReserved
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

nonReserved
    : ALTER | TABLE | SET | POLICY | RETENTION | SHARING | REPLICATION | HISTORY
    | GRANT | REVOKE | ON | TO | SHOW | GRANTS | PATTERN | WHERE | COLUMN
    ;

sharingPolicy
    : SHARING '=' BOOLEAN
    ;

BOOLEAN
    : 'TRUE' | 'FALSE'
    ;

retentionPolicy
    : RETENTION '=' duration
    ;

columnRetentionPolicy
    : ON columnNameClause (columnRetentionPolicyPatternClause)?
    ;

replication
    : REPLICATION
    ;

replicationPolicy
    : replication '=' tableReplicationPolicy
    ;

tableReplicationPolicy
    : '(' replicationPolicyClause (',' replicationPolicyClause)* ')'
    ;

replicationPolicyClause
    : '{' replicationPolicyClusterClause (',' replicationPolicyIntervalClause)? '}'
    ;

replicationPolicyClusterClause
    : DESTINATION ':' STRING
    ;

replicationPolicyIntervalClause
    : INTERVAL ':' RETENTION_HOUR
    | INTERVAL ':' RETENTION_DAY
    ;

columnRetentionPolicyPatternClause
    : WHERE retentionColumnPatternClause
    ;

columnNameClause
    : COLUMN identifier
    ;

retentionColumnPatternClause
    : PATTERN '=' STRING
    ;

duration
    : RETENTION_DAY
    | RETENTION_YEAR
    | RETENTION_MONTH
    | RETENTION_HOUR
    ;

RETENTION_DAY
    : POSITIVE_INTEGER 'D'
    ;

RETENTION_YEAR
    : POSITIVE_INTEGER 'Y'
    ;

RETENTION_MONTH
    : POSITIVE_INTEGER 'M'
    ;

RETENTION_HOUR
    : POSITIVE_INTEGER 'H'
    ;

columnPolicy
    : TAG '=' multiTagIdentifier
    | TAG '=' '(' NONE ')'
    ;

multiTagIdentifier
    : '(' policyTag (',' policyTag)* ')'
    ;

policyTag
    : PII | HC
    ;

historyPolicy
    : HISTORY maxAge? versions?
    ;

maxAge
    : MAX_AGE'='duration
    ;

versions
    : VERSIONS'='POSITIVE_INTEGER
    ;

ALTER: 'ALTER';
TABLE: 'TABLE';
SET: 'SET';
UNSET: 'UNSET';
POLICY: 'POLICY';
RETENTION: 'RETENTION';
REPLICATION: 'REPLICATION';
HISTORY: 'HISTORY';
SHARING: 'SHARING';
GRANT: 'GRANT';
REVOKE: 'REVOKE';
ON: 'ON';
TO: 'TO';
FROM: 'FROM';
SELECT: 'SELECT';
DESCRIBE: 'DESCRIBE';
GRANT_REVOKE: 'MANAGE GRANTS';
CREATE_TABLE: 'CREATE TABLE';
DATABASE: 'DATABASE';
SHOW: 'SHOW';
GRANTS: 'GRANTS';
PATTERN: 'PATTERN';
DESTINATION: 'DESTINATION';
INTERVAL: 'INTERVAL';
WHERE: 'WHERE';
COLUMN: 'COLUMN';
PII: 'PII';
HC: 'HC';
MODIFY: 'MODIFY';
TAG: 'TAG';
NONE: 'NONE';
VERSIONS: 'VERSIONS';
MAX_AGE: 'MAX_AGE';

POSITIVE_INTEGER
    : DIGIT+
    ;

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' {!isHint()}? (BRACKETED_COMMENT|.)*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;