/****** Object:  StoredProcedure [dbo].[sp_YelloTaxiTripSpeedInsight]    Script Date: 10-01-2021 23:06:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[sp_YelloTaxiTripSpeedInsight]
(
	@tripyear			NVARCHAR(50) = NULL	,
	@tripmonth		    NVARCHAR(100) = NULL,
	@tripday            NVARCHAR(100) = NULL
)
AS
BEGIN
	SET NOCOUNT ON;  
 
	DECLARE @SQL				 VARCHAR(MAX)
	DECLARE @tripyearFilter		 VARCHAR(MAX)
	DECLARE @tripmonthFilter	 VARCHAR(MAX)
	DECLARE @tripdayFilter	 VARCHAR(MAX)
	DECLARE @all                 VARCHAR(2)   = '-1'

	SET @tripyearFilter = CASE WHEN @tripyear IS NULL OR @tripyear = 0 
	THEN '''' + @all + ''' = ''' + @all + '''' 
	ELSE 'trip_year = ''' +  @tripyear + '''' 
	END

	SET @tripmonthFilter = CASE WHEN @tripmonth IS NULL OR @tripyear = 0 
	THEN '''' + @all + ''' = ''' + @all + '''' 
	ELSE 'trip_month = ''' +  @tripmonth + '''' 
	END

	SET @tripdayFilter = CASE WHEN @tripday IS NULL OR @tripyear = 0 
	THEN '''' + @all + ''' = ''' + @all + '''' 
	ELSE 'trip_day_of_month = ''' +  @tripday + '''' 
	END

	SET @SQL = 'SELECT trip_hour_of_day,max_trip_speed FROM [dbo].[YellowTaxiTripSpeedDetails]
			WHERE ' + @tripyearFilter + ' AND '+ @tripmonthFilter +' AND ' + @tripdayFilter + ''
			
 
	PRINT (@sql)
	EXEC(@sql)
END