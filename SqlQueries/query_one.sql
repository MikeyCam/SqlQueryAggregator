SELECT [PostalCode]
      ,COUNT ([PostalCode]) AS ResidentsPerArea
  FROM [AdventureWorks2017].[Person].[Address]
  GROUP BY PostalCode