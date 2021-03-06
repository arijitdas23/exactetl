/****** Object:  StoredProcedure [dbo].[sp_YelloTaxiTipInsight]    Script Date: 10-01-2021 23:03:59 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [dbo].[sp_YelloTaxiTipInsight]
(
	@tripyear			NVARCHAR(50) = NULL	,
	@tripquarter		NVARCHAR(100) = NULL
)
AS
BEGIN
	SET NOCOUNT ON;  
 
	DECLARE @SQL				 VARCHAR(MAX)
	DECLARE @tripyearFilter		 VARCHAR(MAX)
	DECLARE @tripquarterFilter	 VARCHAR(MAX)
	DECLARE @all                 VARCHAR(2)   = '-1'

	SET @tripyearFilter = CASE WHEN @tripyear IS NULL OR @tripyear = 0 
	THEN '''' + @all + ''' = ''' + @all + '''' 
	ELSE 'trip_year = ''' +  @tripyear + '''' 
	END

	SET @tripquarterFilter = CASE WHEN @tripquarter IS NULL OR @tripyear = 0 
	THEN '''' + @all + ''' = ''' + @all + '''' 
	ELSE 'trip_quarter = ''' +  @tripquarter + '''' 
	END

	SET @SQL = 'SELECT trip_year,trip_quarter,max_tip_pctg FROM [dbo].[YellowTaxiTipPctg]
			WHERE ' + @tripyearFilter + ' AND ' + @tripquarterFilter + ''
			
 
	EXEC(@sql)
END