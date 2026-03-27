# Task: Table Storage Location Federation

## Phase 1: StorageLocation Service (HTS schema + new service)
- [x] 1. Add `storage_location` and `table_storage_location` tables to HTS schema.sql
- [x] 2. Create `StorageLocationRow.java` JPA entity
- [x] 3. Create `StorageLocationJdbcRepository.java`
- [x] 4. Create `TableStorageLocationRow.java` + `TableStorageLocationRowPrimaryKey.java` JPA entity
- [x] 5. Create `TableStorageLocationJdbcRepository.java`
- [x] 6. Create `StorageLocationService.java` interface + `StorageLocationServiceImpl.java`
       (explicit bean via `StorageLocationConfig.java` — component scan skipped impl class)
- [x] 7. Create `StorageLocationsController.java` (POST + GET + GET-for-table + link endpoints)

## Phase 2: Table API changes
- [x] 8. Add `storageLocations` to `GetTableResponseBody.java`
- [x] 9. Create `UpdateStorageLocationRequestBody.java`
- [x] 10. Add PATCH endpoint to `TablesController.java`
- [x] 11. Add handler method to `TablesApiHandler.java` + `OpenHouseTablesApiHandler.java`
- [x] 12. Add service method to `TablesServiceImpl.java` (updateStorageLocation)
- [x] 13. Add `swapStorageLocation` to `OpenHouseInternalRepository` + impl
- [x] 14. `StorageLocationRepository` + `StorageLocationRepositoryImpl` in internal catalog
         (explicit bean via `StorageLocationConfig.java` in catalog package)
- [x] 15. `HouseTable.java` — StorageLocation surfacing done via API response, not entity field

## Phase 3: Tests + Verification
- [x] 16. Fix `MockTablesApiHandler.java` — add `updateStorageLocation` stub
- [x] 17. Fix `MockTablesApplication.java` — add `@MockBean StorageLocationRepository`
- [x] 18. `getTable()` made fault-tolerant — empty list on HTS error, log warning
- [x] 19. Run `./gradlew :services:housetables:test` — PASSED
- [x] 20. Run `./gradlew :services:tables:test` — PASSED
