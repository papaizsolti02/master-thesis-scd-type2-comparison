-- -----------------------------------------------------------------------------
-- Author: Csaba-Zsolt Papai
-- Date: 2026-04-18
-- Name: snapshot_scd2.usp_MergeUsers
-- Description: Snapshot SCD2 merge from snapshot_scd2.stage_Users to snapshot_scd2.prod_Users.
-- Version: 1.0
-- -----------------------------------------------------------------------------

CREATE PROCEDURE [snapshot_scd2].[usp_MergeUsers]
	@PipelineRunId NVARCHAR(128) = NULL
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @NormalizedPipelineRunId NVARCHAR(128) = NULLIF(TRIM(@PipelineRunId), '');
	DECLARE @AsOfDate DATETIME = CURRENT_TIMESTAMP;
	DECLARE @MatchedCount INT = 0;
	DECLARE @ExpiredCount INT = 0;
	DECLARE @InsertedCount INT = 0;

	DECLARE @RowsRead BIGINT = 0;
	DECLARE @RowsScanned BIGINT = 0;
	DECLARE @RowsWritten BIGINT = 0;
	DECLARE @RowsInserted BIGINT = 0;
	DECLARE @RowsUpdated BIGINT = 0;
	DECLARE @RowsExpired BIGINT = 0;

	DECLARE @ProcStartUtc DATETIME2(3) = SYSUTCDATETIME();
	DECLARE @ProcEndUtc DATETIME2(3) = NULL;
	DECLARE @ProcObjectId INT = OBJECT_ID(N'[snapshot_scd2].[usp_MergeUsers]');

	DECLARE @CpuDelta BIGINT = NULL;
	DECLARE @LogicalReadsDelta BIGINT = NULL;
	DECLARE @PhysicalReadsDelta BIGINT = NULL;
	DECLARE @WritesDelta BIGINT = NULL;

	DECLARE @ErrorNumber INT = NULL;
	DECLARE @ErrorMessage NVARCHAR(4000) = NULL;

	IF OBJECT_ID(N'[snapshot_scd2].[stage_Users]', N'U') IS NULL
	BEGIN
		THROW 55201, 'Required table [snapshot_scd2].[stage_Users] does not exist.', 1;
	END;

	IF OBJECT_ID(N'[snapshot_scd2].[prod_Users]', N'U') IS NULL
	BEGIN
		THROW 55202, 'Required table [snapshot_scd2].[prod_Users] does not exist.', 1;
	END;

	IF @NormalizedPipelineRunId IS NOT NULL
	BEGIN
		EXEC [monitor].[usp_ProcedureRunStart]
			@PipelineRunId = @NormalizedPipelineRunId,
			@ProcedureName = N'snapshot_scd2.usp_MergeUsers',
			@ProcedurePhase = N'Merge';
	END;

	BEGIN TRY
		BEGIN TRAN;

		SELECT @RowsRead = COUNT_BIG(1)
		FROM [snapshot_scd2].[stage_Users];

		UPDATE p
		SET
			p.[IsActive] = 0,
			p.[ExpirationDate] = @AsOfDate,
			p.[LastRefreshedDate] = @AsOfDate
		FROM [snapshot_scd2].[prod_Users] AS p
		WHERE p.[IsActive] = 1
			AND NOT EXISTS
			(
				SELECT 1
				FROM [snapshot_scd2].[stage_Users] AS s
				WHERE ISNULL(s.[Rowhash], 0x0) = ISNULL(p.[Rowhash], 0x0)
			);

		SET @ExpiredCount = @@ROWCOUNT;

		INSERT INTO [snapshot_scd2].[prod_Users]
		(
			[FullName],
			[FirstName],
			[LastName],
			[Email],
			[Username],
			[DateOfBirth],
			[YearOfBirth],
			[MonthOfBirth],
			[DayOfBirth],
			[RegistrationDate],
			[Country],
			[CountryCode],
			[City],
			[Gender],
			[AccountCreatedVia],
			[ReferralSource],
			[SubscriptionTier],
			[SubscriptionTierRank],
			[IsPaidTier],
			[BillingCycle],
			[PaymentMethod],
			[PaymentMethodGroup],
			[IsCardBased],
			[AutoRenew],
			[MarketingConsent],
			[PreferredLanguage],
			[ContentLanguage],
			[PlanAddons],
			[LastRefreshedDate],
			[EffectiveDate],
			[ExpirationDate],
			[IsActive],
			[Hashdata],
			[Rowhash]
		)
		SELECT
			s.[FullName],
			s.[FirstName],
			s.[LastName],
			s.[Email],
			s.[Username],
			s.[DateOfBirth],
			s.[YearOfBirth],
			s.[MonthOfBirth],
			s.[DayOfBirth],
			s.[RegistrationDate],
			s.[Country],
			s.[CountryCode],
			s.[City],
			s.[Gender],
			s.[AccountCreatedVia],
			s.[ReferralSource],
			s.[SubscriptionTier],
			s.[SubscriptionTierRank],
			s.[IsPaidTier],
			s.[BillingCycle],
			s.[PaymentMethod],
			s.[PaymentMethodGroup],
			s.[IsCardBased],
			s.[AutoRenew],
			s.[MarketingConsent],
			s.[PreferredLanguage],
			s.[ContentLanguage],
			s.[PlanAddons],
			@AsOfDate,
			@AsOfDate,
			'9999-12-31 00:00:00',
			1,
			s.[Hashdata],
			s.[Rowhash]
		FROM [snapshot_scd2].[stage_Users] AS s
		LEFT JOIN [snapshot_scd2].[prod_Users] AS p
			ON ISNULL(p.[Rowhash], 0x0) = ISNULL(s.[Rowhash], 0x0)
			AND p.[IsActive] = 1
		WHERE p.[Id] IS NULL;

		SET @InsertedCount = @@ROWCOUNT;
		SET @RowsInserted = @InsertedCount;
		SET @RowsWritten = @InsertedCount + @ExpiredCount;
		SET @RowsExpired = @ExpiredCount;

		UPDATE p
		SET p.[LastRefreshedDate] = @AsOfDate
		FROM [snapshot_scd2].[prod_Users] AS p;

		SET @RowsUpdated = CAST(@MatchedCount AS BIGINT);
		SET @RowsScanned = @RowsRead;

		COMMIT TRAN;

		SET @ProcEndUtc = SYSUTCDATETIME();

		IF EXISTS (
			SELECT 1
			FROM [sys].[database_query_store_options] AS qo
			WHERE qo.[actual_state_desc] IN ('READ_WRITE', 'READ_ONLY')
		)
		BEGIN
			BEGIN TRY
				SELECT
					@CpuDelta = CAST(SUM(CAST(rs.[avg_cpu_time] AS DECIMAL(38, 6)) * rs.[count_executions]) / 1000.0 AS BIGINT),
					@LogicalReadsDelta = CAST(SUM(CAST(rs.[avg_logical_io_reads] AS DECIMAL(38, 6)) * rs.[count_executions]) AS BIGINT),
					@PhysicalReadsDelta = CAST(SUM(CAST(rs.[avg_physical_io_reads] AS DECIMAL(38, 6)) * rs.[count_executions]) AS BIGINT),
					@WritesDelta = CAST(SUM(CAST(rs.[avg_logical_io_writes] AS DECIMAL(38, 6)) * rs.[count_executions]) AS BIGINT)
				FROM [sys].[query_store_query] AS q
				INNER JOIN [sys].[query_store_plan] AS p
					ON q.[query_id] = p.[query_id]
				INNER JOIN [sys].[query_store_runtime_stats] AS rs
					ON p.[plan_id] = rs.[plan_id]
				INNER JOIN [sys].[query_store_runtime_stats_interval] AS rsi
					ON rs.[runtime_stats_interval_id] = rsi.[runtime_stats_interval_id]
				WHERE q.[object_id] = @ProcObjectId
					AND rsi.[start_time] < @ProcEndUtc
					AND rsi.[end_time] > @ProcStartUtc;
			END TRY
			BEGIN CATCH
				SET @CpuDelta = NULL;
				SET @LogicalReadsDelta = NULL;
				SET @PhysicalReadsDelta = NULL;
				SET @WritesDelta = NULL;
			END CATCH;
		END;

		IF @NormalizedPipelineRunId IS NOT NULL
		BEGIN
			EXEC [monitor].[usp_ProcedureRunFinish]
				@PipelineRunId = @NormalizedPipelineRunId,
				@ProcedureName = N'snapshot_scd2.usp_MergeUsers',
				@Status = N'Succeeded',
				@RowsRead = @RowsRead,
				@RowsScanned = @RowsScanned,
				@RowsWritten = @RowsWritten,
				@RowsInserted = @RowsInserted,
				@RowsUpdated = @RowsUpdated,
				@RowsExpired = @RowsExpired,
				@CpuTimeMs = @CpuDelta,
				@LogicalReads = @LogicalReadsDelta,
				@PhysicalReads = @PhysicalReadsDelta,
				@Writes = @WritesDelta;
		END;

		SELECT
			@AsOfDate AS [MergeTimestamp],
			@MatchedCount AS [MatchedRows],
			@ExpiredCount AS [ExpiredRows],
			@InsertedCount AS [InsertedRows];
	END TRY
	BEGIN CATCH
		SET @ErrorNumber = ERROR_NUMBER();
		SET @ErrorMessage = ERROR_MESSAGE();

		IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRAN;
		END;

		SET @ProcEndUtc = SYSUTCDATETIME();

		IF EXISTS (
			SELECT 1
			FROM [sys].[database_query_store_options] AS qo
			WHERE qo.[actual_state_desc] IN ('READ_WRITE', 'READ_ONLY')
		)
		BEGIN
			BEGIN TRY
				SELECT
					@CpuDelta = CAST(SUM(CAST(rs.[avg_cpu_time] AS DECIMAL(38, 6)) * rs.[count_executions]) / 1000.0 AS BIGINT),
					@LogicalReadsDelta = CAST(SUM(CAST(rs.[avg_logical_io_reads] AS DECIMAL(38, 6)) * rs.[count_executions]) AS BIGINT),
					@PhysicalReadsDelta = CAST(SUM(CAST(rs.[avg_physical_io_reads] AS DECIMAL(38, 6)) * rs.[count_executions]) AS BIGINT),
					@WritesDelta = CAST(SUM(CAST(rs.[avg_logical_io_writes] AS DECIMAL(38, 6)) * rs.[count_executions]) AS BIGINT)
				FROM [sys].[query_store_query] AS q
				INNER JOIN [sys].[query_store_plan] AS p
					ON q.[query_id] = p.[query_id]
				INNER JOIN [sys].[query_store_runtime_stats] AS rs
					ON p.[plan_id] = rs.[plan_id]
				INNER JOIN [sys].[query_store_runtime_stats_interval] AS rsi
					ON rs.[runtime_stats_interval_id] = rsi.[runtime_stats_interval_id]
				WHERE q.[object_id] = @ProcObjectId
					AND rsi.[start_time] < @ProcEndUtc
					AND rsi.[end_time] > @ProcStartUtc;
			END TRY
			BEGIN CATCH
				SET @CpuDelta = NULL;
				SET @LogicalReadsDelta = NULL;
				SET @PhysicalReadsDelta = NULL;
				SET @WritesDelta = NULL;
			END CATCH;
		END;

		IF @RowsScanned = 0
		BEGIN
			SET @RowsScanned = @RowsRead;
		END;

		IF @NormalizedPipelineRunId IS NOT NULL
		BEGIN
			BEGIN TRY
				EXEC [monitor].[usp_ProcedureRunFinish]
					@PipelineRunId = @NormalizedPipelineRunId,
					@ProcedureName = N'snapshot_scd2.usp_MergeUsers',
					@Status = N'Failed',
					@RowsRead = @RowsRead,
					@RowsScanned = @RowsScanned,
					@RowsWritten = @RowsWritten,
					@RowsInserted = @RowsInserted,
					@RowsUpdated = @RowsUpdated,
					@RowsExpired = @RowsExpired,
					@CpuTimeMs = @CpuDelta,
					@LogicalReads = @LogicalReadsDelta,
					@PhysicalReads = @PhysicalReadsDelta,
					@Writes = @WritesDelta,
					@ErrorNumber = @ErrorNumber,
					@ErrorMessage = @ErrorMessage;
			END TRY
			BEGIN CATCH
			END CATCH;
		END;

		THROW;
	END CATCH;
END;
GO
