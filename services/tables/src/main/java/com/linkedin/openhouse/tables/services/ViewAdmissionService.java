package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;

/**
 * Admission seam for view create/replace. Invoked by {@code ViewsServiceImpl} after authorization
 * and base-version checking, and before the repository commit, storage allocation, or any file
 * write, so a future admission failure allocates and writes nothing.
 *
 * <p>M1 ships only the pass-through implementation. Future Spark/Trino validation and Coral
 * generation attach at this seam without changing the commit path.
 */
public interface ViewAdmissionService {

  /**
   * @param requestBody the create/update request being admitted
   * @throws com.linkedin.openhouse.tables.exception.ViewApiException with {@link
   *     com.linkedin.openhouse.tables.exception.ViewErrorCode#VIEW_ADMISSION_FAILED} (or a reserved
   *     dependency-analysis code) if admission rejects the request. Never thrown by the M1
   *     pass-through implementation.
   */
  void admit(CreateUpdateViewRequestBody requestBody);
}
