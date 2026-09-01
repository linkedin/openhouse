package com.linkedin.openhouse.housetables.repository.impl.jdbc;

import static com.linkedin.openhouse.housetables.repository.impl.jdbc.CorruptEntityTypeTranslation.translating;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/**
 * Makes corruption translation structural: every {@link UserTableHtsJdbcRepository} call is
 * translated, so a read added later needs no wrap at its call site to get the diagnostic.
 *
 * <p>Spring Data builds its own proxy around the repository, and {@code
 * PersistenceExceptionTranslationInterceptor} and {@code TransactionInterceptor} live inside that
 * one. This advice lands in a second, outer proxy, so it sees the already-translated {@link
 * org.springframework.dao.DataAccessException} rather than the converter's raw {@code
 * PersistenceException}, and it sits outside the repository's transaction exactly as the call-site
 * wraps it replaced did. That nesting is structural, not an ordering: no {@code @Order} value can
 * move this advice inside Spring Data's chain, which is why none is declared.
 * RepositoryTranslationAspectTest pins both halves.
 *
 * <p>Advising writes too is harmless: {@code translating} rethrows every non-corruption {@code
 * DataAccessException} as the very same instance, so the conflict mapping is untouched.
 */
@Aspect
@Component
public class UserTableRepositoryTranslationAspect {

  @Around(
      "execution(* com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository.*(..))")
  public Object translateCorruption(ProceedingJoinPoint joinPoint) {
    return translating(() -> proceed(joinPoint));
  }

  private static Object proceed(ProceedingJoinPoint joinPoint) throws Exception {
    try {
      return joinPoint.proceed();
    } catch (Exception e) {
      throw e;
    } catch (Error e) {
      throw e;
    } catch (Throwable t) {
      throw new IllegalStateException(t);
    }
  }
}
