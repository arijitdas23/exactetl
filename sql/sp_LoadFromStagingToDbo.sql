CREATE OR ALTER PROCEDURE [dbo].[sp_LoadFromStagingToDbo] AS
BEGIN
	DECLARE @source varchar(50)
	DECLARE @currdate datetime

	set @source = 'sp_LoadFromStagingToDbo'
	set @currdate = getdate()

	BEGIN TRY
		BEGIN TRAN TIPPCTG
			MERGE INTO [dbo].[YellowTaxiTipPctg] tippctg
			USING
			(select distinct trip_year,trip_quarter,tip_percentage,max_tip_pctg,
			concat(trip_year,'-',trip_quarter) as 'TipPctgID',@source LastUpdatedBy ,@currdate LastUpdatedTime 
			from Staging.YellowTaxiTipPctg where processed_status='N') stgtippctg
			ON tippctg.TipPctgID = stgtippctg.TipPctgID
			WHEN MATCHED THEN
				UPDATE SET tippctg.trip_year = stgtippctg.trip_year,
						   tippctg.trip_quarter = stgtippctg.trip_quarter,
						   tippctg.tip_percentage = stgtippctg.tip_percentage,
						   tippctg.max_tip_pctg = stgtippctg.max_tip_pctg,
						   tippctg.LastUpdatedBy = stgtippctg.LastUpdatedBy,
						   tippctg.LastUpdatedTime = stgtippctg.LastUpdatedTime
			WHEN NOT MATCHED THEN
				INSERT (TipPctgID,trip_year,trip_quarter,tip_percentage,max_tip_pctg,LastUpdatedBy,LastUpdatedTime)
				VALUES (TipPctgID,trip_year,trip_quarter,tip_percentage,max_tip_pctg,LastUpdatedBy,LastUpdatedTime);

		COMMIT TRAN TIPPCTG
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN TIPPCTG
	END CATCH

	BEGIN TRY
		BEGIN TRAN TIPPCTGDET
			INSERT INTO dbo.YellowTaxiTipPctgDetails (TipPctgID,
													VendorID,
													tpep_pickup_datetime,
													tpep_dropoff_datetime,
													trip_date,
													trip_year,
													trip_quarter,
													passenger_count,
													trip_distance,
													RatecodeID,
													store_and_fwd_flag ,
													PULocationID ,
													DOLocationID,
													payment_type,
													fare_amount  ,
													extra ,
													mta_tax ,
													tip_amount ,
													tolls_amount ,
													improvement_surcharge ,
													total_amount ,
													congestion_surcharge ,
													tip_percentage ,
													max_tip_pctg ,
													LastUpdatedBy ,
													LastUpdatedTime)
			SELECT concat(trip_year,'-',trip_quarter) as 'TipPctgID',VendorID,tpep_pickup_datetime,
			tpep_dropoff_datetime,trip_date,trip_year,
			trip_quarter,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag ,PULocationID ,
			DOLocationID,payment_type,fare_amount  ,extra ,mta_tax ,tip_amount ,tolls_amount ,
			improvement_surcharge ,total_amount ,congestion_surcharge ,tip_percentage ,max_tip_pctg ,
			@source LastUpdatedBy , @currdate LastUpdatedTime
			FROM 
			Staging.YellowTaxiTipPctg
			WHERE processed_status='N'

			UPDATE Staging.YellowTaxiTipPctg SET processed_status = 'Y'
			WHERE processed_status = 'N'
		COMMIT TRAN TIPPCTGDET
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN TIPPCTGDET
	END CATCH

	BEGIN TRY
		BEGIN TRAN TRIPSPEED
			MERGE INTO [dbo].[YellowTaxiTripSpeed] tripspeed
			USING
			(select distinct trip_date,trip_year,trip_quarter,trip_month,trip_day_of_month,
			trip_hour_of_day,trip_speed,max_trip_speed,
			concat(trip_year,'-',trip_month,'-',trip_day_of_month,'-',trip_hour_of_day) as 'TripSpeedID',
			@source LastUpdatedBy ,@currdate LastUpdatedTime 
			from Staging.YellowTaxiTripSpeed where processed_status='N') stgtripspeed
			ON tripspeed.TripSpeedID = stgtripspeed.TripSpeedID
			WHEN MATCHED THEN
				UPDATE SET tripspeed.trip_year = stgtripspeed.trip_year,
						   tripspeed.trip_date = stgtripspeed.trip_date,
						   tripspeed.trip_quarter = stgtripspeed.trip_quarter,
						   tripspeed.trip_month = stgtripspeed.trip_month,
						   tripspeed.trip_day_of_month = stgtripspeed.trip_day_of_month,
						   tripspeed.trip_hour_of_day = stgtripspeed.trip_hour_of_day,
						   tripspeed.trip_speed = stgtripspeed.trip_speed,
						   tripspeed.max_trip_speed = stgtripspeed.max_trip_speed,
						   tripspeed.LastUpdatedBy = stgtripspeed.LastUpdatedBy,
						   tripspeed.LastUpdatedTime = stgtripspeed.LastUpdatedTime
			WHEN NOT MATCHED THEN
				INSERT (TripSpeedID,trip_year,trip_date,trip_quarter,trip_month,trip_day_of_month,
				trip_hour_of_day,trip_speed,max_trip_speed,LastUpdatedBy,LastUpdatedTime)
				VALUES (TripSpeedID,trip_year,trip_date,trip_quarter,trip_month,trip_day_of_month,
				trip_hour_of_day,trip_speed,max_trip_speed,LastUpdatedBy,LastUpdatedTime);

		COMMIT TRAN TRIPSPEED
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN TRIPSPEED
	END CATCH

	BEGIN TRY
		BEGIN TRAN TRIPSPEEDDET
			INSERT INTO dbo.YellowTaxiTripSpeedDetails (TripSpeedID,
													VendorID,
													tpep_pickup_datetime,
													tpep_dropoff_datetime,
													trip_date,
													trip_year,
													trip_quarter,
													trip_month,
													trip_day_of_month,
													trip_hour_of_day,
													passenger_count,
													trip_distance,
													RatecodeID,
													store_and_fwd_flag ,
													PULocationID ,
													DOLocationID,
													payment_type,
													fare_amount  ,
													extra ,
													mta_tax ,
													tip_amount ,
													tolls_amount ,
													improvement_surcharge ,
													total_amount ,
													congestion_surcharge ,
													trip_duration_in_hour ,
													trip_speed ,
													max_trip_speed,
													LastUpdatedBy ,
													LastUpdatedTime)
			SELECT concat(trip_year,'-',trip_month,'-',trip_day_of_month,'-',trip_hour_of_day) as 'TripSpeedID',
			VendorID,tpep_pickup_datetime,
			tpep_dropoff_datetime,trip_date,trip_year,
			trip_quarter,trip_month,trip_day_of_month,trip_hour_of_day,passenger_count,trip_distance,
			RatecodeID,store_and_fwd_flag ,PULocationID ,
			DOLocationID,payment_type,fare_amount  ,extra ,mta_tax ,tip_amount ,tolls_amount ,
			improvement_surcharge ,total_amount ,congestion_surcharge ,trip_duration_in_hour ,trip_speed ,max_trip_speed,
			@source LastUpdatedBy , @currdate LastUpdatedTime
			FROM 
			Staging.YellowTaxiTripSpeed
			WHERE processed_status='N'

			UPDATE Staging.YellowTaxiTripSpeed SET processed_status = 'Y'
			WHERE processed_status = 'N'
		COMMIT TRAN TRIPSPEEDDET
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN TRIPSPEEDDET
	END CATCH


END
			