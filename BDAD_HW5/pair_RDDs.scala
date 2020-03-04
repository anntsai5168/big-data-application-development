// input data
val mydata = sc.textFile("loudacre/weblog/2014-03-15.log")

// a. Count the number of requests from each user and save the result to an RDD named: setupCountsRDD
val userID = mydata.map(line => line.split(' ')(2))
val setupCountsRDD = userID.map(field => (field, 1))

// b. Sum the values for each user ID and save the result to an RDD named: requestCountsRDD
val requestCountsRDD = counts.reduceByKey((v1, v2) => v1+v2)


// c. Determine how many users visited once, twice, three times, etc. and save the result to an RDD named: visitFrequencyTotalsRDD
val reMap = requestCountsRDD.map{case (usedID, count) => (count, 1)}    // (2,1) (5,1)
val count = reMap.reduceByKey((v1, v2) => v1+v2)
val visitFrequencyTotalsRDD = count.map{case (a, b) => "("+a.toString + ":" + b.toString +")"}  // (4:445)


// d. Create an RDD where the user id is the key, and the value is the list of all the IP addresses that the user has connected from. 
//    Save the result to an RDD named: validAcctsIpsFinalRDD
//    only output ip addresses for user IDs that appear in the accounts files.

// dataset 1
val usedIP = mydata.map(line => line.split(' ')).map(fields => (fields(2), fields(0)))
val usedList = usedIP.groupByKey().map(x => (x._1, x._2.toList)) // turn CompactBuffer[returned by GroupBy] into List

// dataset 2
val mydata2 = sc.textFile("loudacre/accounts")
val userID2 = mydata2.map(line => line.split(',')(0)).keyBy(field => field)
val validAcctsIpsFinalRDD = usedList.join(userID2).map{case (userID, (lst, userID2)) => (userID,lst)}



// .take(5).foreach(println)

