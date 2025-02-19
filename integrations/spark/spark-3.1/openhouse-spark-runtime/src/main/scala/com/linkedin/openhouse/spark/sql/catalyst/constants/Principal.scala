package com.linkedin.openhouse.spark.sql.catalyst.constants

/**
 * This object is used to represent keyword global user permission "PUBLIC" which maps to the acl policy representation "*"
 */
object Principal {
  private val PUBLIC_USER = "PUBLIC"
  private val PUBLIC_USER_ACL = "*"
  def apply(principal: String): String = principal match {
    case PUBLIC_USER => PUBLIC_USER_ACL
    case _ => principal
  }

  def unapply(principalAcl: String): Option[String] = principalAcl match {
    case PUBLIC_USER_ACL => Some(PUBLIC_USER)
    case _ => Some(principalAcl)
  }
}
