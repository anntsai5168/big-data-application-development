import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql._


def processCompany(text: String): String = {
    //normalize company name by regexp
    var lower = text.toLowerCase()
    var symbols = """[\.&,-/\*^@#&$%!\?]""".r//deal with space!
    lower = symbols.replaceAllIn(lower, "")
    
    var paren = """\(.*\)""".r
    lower = paren.replaceAllIn(lower, "")
   
    val suffix = """(inc)|(llc)|(limited)|(incorporation)|(incorporated)|(corporate)|(corporation)|(orporated)|(company)|(lp)|(llp)|(ltd)|(university)|(associate[s]?)|( com )|( co )|( corp )|( com,)|( co,)|( corp,)|(corp)|(group)|(holding)|(lab[s]?)""".r
    lower = suffix.replaceAllIn(lower, "") 

    val services = """(service[s]?)|(online)|(system[s]?)|(solution[s]?)|(data)|(software)|(infotech)|(digital)|([\s,]info[\s,])|([\s]soft)|(web)|(solns)""".r
    lower = services.replaceAllIn(lower, "")
    
    val industry = """(consulting)|(financial)|(technolog(y|(ies)))|(communication[s]?)|(entertainment)|([\s,]it[$\s,])|(information)|(business)|(network[s]?)|(resource[s]?)|(consultancy)|(svcs)|(tech)""".r
    lower = industry.replaceAllIn(lower, "")

    val location = """(international)|(global)|(america[s]?)|([,\s]us[,\s])|([,\s]us[a]?)""".r
    lower = location.replaceAllIn(lower, "")
    return lower.trim()
}
def processTitle(text: String): String ={
    //process job title
    var lower = text.toLowerCase()
    if (lower == "machine learning engineer"){
        lower = "data engineer"
    }
    return lower
}

//load data
val skills = "cleaned_jobs"
val salary = "h1b_salary_clean.txt"
val h1b = "final_project/data/cleaned_data"

val skillRdd = sc.textFile(skills)
val salaryRdd = sc.textFile(salary)
val h1bdatafram = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv(h1b)

//h1bdata group by employer change column name
val h1bdatafram2 = h1bdatafram.withColumn("TotalApproval", col("Initial Approvals") + col("Continuing Approvals")).withColumn("TotalDenial", col("Initial Denials") + col("Continuing Denials")).withColumn("TotalApply", col("TotalApproval") + col("TotalDenial"))
val AR_Employer = h1bdatafram2.filter(col("Fiscal Year")>2011).groupBy("Employer").sum("TotalApproval","TotalDenial","TotalApply").withColumn("ApprovalRate", col("sum(TotalApproval)")/col("sum(TotalApply)"))sort(col("sum(TotalApply)").desc)
val AR_EmployerRDD = AR_Employer.rdd
val AR_EmployerPRDD = AR_EmployerRDD.keyBy(line => processCompany(line.getString(0))).map(line => (line._1, Row((line._2(1),line._2(4)))))//count:206848

//salary_cleaned to rdd and process through map function
val salaryPRDD = salaryRdd.map(line => line.split(",")).keyBy(line => processCompany(line(0)))//count 257904 
val skillPRDD = skillRdd.map(line => line.replace("(", "").replace(")", "").split(",")).keyBy(line => processCompany(line(0))) // count 1210

val salary_cleaned = salaryRdd.map(line => line.split(",")).map(line => ((processCompany(line(0)), processTitle(line(1))), (line(2), line(3), line(4), line(5), line(6))))
val skill_cleaned = skillRdd.map(line => line.replace("(", "").replace(")", "").split(",")).map(line => ((processCompany(line(0)), processTitle(line(1))), (line(2), line(3))))

//join two dataset using rdd
val joined = salary_cleaned.join(skill_cleaned)

val joined2 = salaryPRDD.join(AR_EmployerPRDD)//1437509  

//saved file
joined2.saveAsTextFile("salary_h1b_joined")