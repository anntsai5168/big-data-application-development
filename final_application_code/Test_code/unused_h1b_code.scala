//load in data
val dir = "final_project/h1b_acceptance/cleaned_h1bdata"
val cdata = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(dir)

//Check TotalApproval & TotalDenial
val cdata2 = cdata.withColumn("TotalApproval", col("Initial Approvals") + col("Continuing Approvals")).withColumn("TotalDenial", col("Initial Denials") + col("Continuing Denials")).withColumn("TotalApply", col("TotalApproval") + col("TotalDenial"))

//Groupby emplyer, Fiscal Year
val Employer = cdata2.groupBy("Fiscal Year","Employer").sum("TotalApproval","TotalDenial","TotalApply").sort(col("Fiscal Year"),col("sum(TotalApply)").desc)
//Groupby NAICS, Fiscal Year
val State = cdata2.groupBy("Fiscal Year","State").sum("TotalApproval","TotalDenial","TotalApply").sort(col("Fiscal Year"),col("sum(TotalApply)").desc)
//Groupby State, Fiscal Year
val NAICS = cdata2.groupBy("Fiscal Year","NAICS").sum("TotalApproval","TotalDenial","TotalApply").sort(col("Fiscal Year"),col("sum(TotalApply)").desc)

// val Employer = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("final_project/data/Employer")
// val State = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("final_project/data/State")
// val NAICS = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("final_project/data/NAICS")

val AR_Employer = cdata2.filter(col("Fiscal Year")>2011).groupBy("Employer").sum("TotalApproval","TotalDenial","TotalApply").withColumn("ApprovalRate", col("sum(TotalApproval)")/col("sum(TotalApply)"))sort(col("sum(TotalApply)").desc)
val AR_State = cdata2.filter(col("Fiscal Year")>2011).groupBy("State").sum("TotalApproval","TotalDenial","TotalApply").withColumn("ApprovalRate", col("sum(TotalApproval)")/col("sum(TotalApply)"))sort(col("sum(TotalApply)").desc)
val AR_NAICS = cdata2.filter(col("Fiscal Year")>2011).groupBy("NAICS").sum("TotalApproval","TotalDenial","TotalApply").withColumn("ApprovalRate", col("sum(TotalApproval)")/col("sum(TotalApply)"))sort(col("sum(TotalApply)").desc)

//Save
AR_Employer.coalesce(1).write.option("header","true").option("sep",",").format("csv").save("final_project/data/AR_Employer2")
AR_State.coalesce(1).write.option("header","true").option("sep",",").format("csv").save("final_project/data/AR_State2")
AR_NAICS.coalesce(1).write.option("header","true").option("sep",",").format("csv").save("final_project/data/AR_NAICS2")

//Load 
// val AR_Employer = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("final_project/data/AR_Employer2")
// val AR_State = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("final_project/data/AR_State2")
// val AR_NAICS = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("final_project/data/AR_NAICS2")

