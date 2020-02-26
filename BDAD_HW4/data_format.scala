import scala.io.Source
import java.io._
import scala.io.BufferedSource


object data_format {

    def main(args:Array[String]){  
        parse("devicestatus.txt") 
    }  

    def resource(filename: String): BufferedSource = {
        val source = Source.fromFile(filename)
        return source
    }

    def split_advanced(delimiter:String, line:String): Array[String] = {
        if (delimiter == "|"){
            return line.split("""\|""")
        }
        else{
            return line.split(delimiter)
        }
    }

    def output(date: String, manufacturer: String, device_ID: String, latitude: String, longitude: String): String = {
        val log =  date + "," + manufacturer + "," + device_ID + "," + latitude + "," + longitude
        return log
    }

    def parse(input: String): Unit = {

        val file = new File("data_format_output.txt")
        val bw = new BufferedWriter(new FileWriter(file))

        // a. Load the dataset
        val source = resource(input)
        
        for (line <- source.getLines) { // cannot use rdd here, cuz  getLines is not a member of org.apache.spark.rdd.RDD[String]
            
            // b. Determine which delimiter to use - hint: the character at position 19 (the 20th character) is the first use of the delimiter.
            val delimiter = line.slice(19, 20) // return String
            val arr = split_advanced(delimiter, line)
            
            // c. Filter out any records which do not parse correctly - hint: each record should have exactly 14 values.
            val len = arr.length
            if (len == 14){

                // d. Extract the date (first field), mfr_model (second field), device ID (third field), and latitude and longitude (13th and 14th fields respectively).
                val date = arr(0)
                val mfr_model = arr(1)
                val device_ID = arr(2)
                val latitude = arr(12)
                val longitude = arr(13)

                // e. The second field (mfr_model) contains the device manufacturer and model name (e.g. “Ronin S2” or “Sorrento F41L”) 
                //    Split this field on the blank(s) to separate the manufacturer from the model (e.g. manufacturer “Ronin”, model “S2”)
                val manufacturer = mfr_model.split(" ")(0)

                // f. Save the extracted data, comma delimited, to text files
                val log = output(date, manufacturer, device_ID, latitude, longitude)
                bw.write(log+"\n") 
            }
        }
        bw.close()
        source.close()
    }
}
