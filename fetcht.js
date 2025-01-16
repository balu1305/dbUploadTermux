const axios = require("axios");
const fs = require("fs");
const { promisify } = require("util");
const mkdir = promisify(fs.mkdir);
const { MongoClient } = require("mongodb");

const MAX_CONCURRENT_REQUESTS = 10; // Set your desired maximum concurrent requests
let concurrentRequests = 0;
const requestQueue = [];

const mongoUrl =
  "mongodb+srv://balu:busdata123%40gmail.com@revenuedata.upijd.mongodb.net/"; // MongoDB connection URL
const dbName = "IntrCityData1"; // Database name
const collectionName = "seatWiseData1"; // Collection name

// Function to fetch data for a given date and return it
async function fetchData(date, serviceId, serviceName) {
  const url = `https://ops-partners.intrcity.com/trip_detail_reports?from_date=${date}&bus_service_id=${serviceId}&commit=Filter`;
  const headers = {
    DNT: "1",
    Referer: "https://ops-partners.intrcity.com/trip_detail_reports",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent":
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "sec-ch-ua":
      '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    Connection: "keep-alive",
    Cookie:
      "WZRK_G=d1d98835800446159204a45a31b69bf3; _gcl_au=1.1.1496531873.1725608276; _clck=vruhox%7C2%7Cfp1%7C0%7C1710; _ga_38D9GW18DE=GS1.1.1725886605.6.1.1725886667.0.0.0; _ga=GA1.2.1655186943.1725608277; __cf_bm=PLwMMFb12sCQm11Phc5VycElP.BcluCUBo4FhZa_xHk-1727952635-1.0.1.1-Z_Q8Y6SGZQc5BrF2wcEPBpc_wcZG8.wIArEg54AZhXAPRTPZudayyGZ7DGTL.Tm2bSwKwjAkochB3b.23.5jdQ; _railyatri_bus_operator_session=FzNcojaurUclyFQ5SPXztnW9QiG7aSSzsy%2BjxVygAjpOLYL0rC8Z%2BvaVDFIoomXEyOdyIiWEOeYAIXHsH0D05kAjd2xQB4tmw%2FeRoFhUrHRRe1ql5BIKda8MoTwjr%2Bk7eEbAkmIZghmRQUboxG82ssRbq6dopM2lEpC9wSRl2eUDsSI3Pw4bExZmW3qLzn9pZssgm8gbcafNRq0S6oKNzqbmgxUPjRt%2BAHEY%2FOcT7CKKJkACnWuZXcs3FgeiuQeIypY5Y0U62RAbh8ioVA02gNo6I0X1WF2B%2BXHDximhw99NPM6BG%2FDXIWnxOuUNh6TaFaJuhIPAbl8BDVmXmYZVB9KtOABUmPleDFqwpG20wyWm8wpfE2DBEbZojRQSQeERevvgL6QXcIXtV0npS%2BjYxuOdshfqRKjgMr56bIGvye5E12IQYp8fscWcocw73olHbdodD9tHoQpUEdQsFDcBHGyO3xIlSDW7arkXdTEmQ%2FRbh8YkPTfotH155vw6Pbj4YMfUPtkWcYSmBN1d3j4%2BpYMWolNSZkuuTzzLFjWWckSKGxc%3D--pJ2DiawREVvAC2UK--E1uoh66ho4rK4M5fQLLthA%3D%3D",
  };

  try {
    const response = await axios.get(url, { headers });
    const html = response.data;

    // Find the start and end index of the table
    const startIndex = html.indexOf('<table class="table">');
    const endIndex = html.indexOf("</table>", startIndex);

    const tableHtml = html.substring(startIndex, endIndex + "</table>".length);

    // Parse the table HTML to extract data
    const rows = tableHtml.match(/<tr>(.*?)<\/tr>/gs) || [];

    const data = [];

    // Loop through each table row except the first one (header row) and the last one (footer row)
    rows.slice(1, -1).forEach((row) => {
      // Extract data from each cell in the row
      const cells = row.match(/<(t[dh]).*?>(.*?)<\/\1>/gs);

      if (cells) {
        const rowData = cells.map((cell) =>
          cell
            .replace(/<\/?[^>]+(>|$)/g, "")
            .trim()
            .replace(/,/g, "|")
        );
        data.push(rowData);
      }
    });

    // Extract total amount from the footer row
    const footerRow = rows[rows.length - 1];
    const footerCells = footerRow.match(/<(t[dh]).*?>(.*?)<\/\1>/gs);
    const totalAmount =
      parseFloat(
        footerCells[4]
          .replace(/<\/?[^>]+(>|$)/g, "")
          .trim()
          .replace(/,/g, "|")
      ) || null;

    // Construct the journey data
    const seats = data.map((row) => ({
      user_route: row[1] || null, // Assuming the second column is user_route
      seats: row[2] || null, // Assuming the third column is seats
      status: row[3] || null, // Assuming the fourth column is status
      net_collection_exc_gst: parseFloat(row[4]) || null, // Assuming the fifth column is net_collection_exc_gst
    }));

    return {
      date: new Date(date).toISOString(),
      total_amount: totalAmount,
      seats: seats,
    };
  } catch (error) {
    console.error(
      `Error fetching data for ${date} - ${serviceId}:`,
      error.message
    );
    return null;
  }
}

// Function to read CSV file synchronously and return an array of objects
function readCSVSync(filePath) {	
  const csvData = fs.readFileSync(filePath, { encoding: "utf-8" });
  const lines = csvData.trim().split("\n");
  const headers = lines.shift().split(",");
  const records = lines.map((line) => {
    const values = line.split(",");
    const record = {};
    headers.forEach((header, index) => {
      record[header.trim()] = values[index].trim();
    });
    return record;
  });
  return records;
}

// Read the CSV file and process each service
async function readCSV() {
  const filePath = "filtered_services.csv";
  const records = readCSVSync(filePath);

  const client = new MongoClient(mongoUrl, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });

  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    let docCount = 0;

    for (const record of records) {
      let { service_id, service_name } = record;
      service_id = parseInt(service_id, 10); // Ensure service_id is an integer
      let today = new Date();
      today.setDate(today.getDate() - 1);
      let yesterday = today.toISOString().split("T")[0];
      const startDate = yesterday;
      const endDate = yesterday;
      // const currentDate = new Date(startDate);
      // const endDateObj = new Date(endDate);
      const currentDate = new Date("2025-01-09");
      const endDateObj = new Date("2025-01-10");
      console.log(service_id, service_name);
      service_name = service_name.replace(/\s/g, "").replace(":", "x");

      const journeys = [];
      while (currentDate <= endDateObj) {
        const dateString = currentDate.toISOString().split("T")[0];
        const journeyData = await fetchData(
          dateString,
          service_id,
          service_name
        );
        if (journeyData) {
          journeys.push(journeyData);
        }
        currentDate.setDate(currentDate.getDate() + 1);
      }

      const document = {
        service_id: service_id,
        source: service_name.split("-")[0],
        destination: service_name.split("-")[1].split("|")[0], // Remove time from destination
        journeys: journeys,
      };

      // Print the document to see the structure
      console.log("Document to be stored:", JSON.stringify(document, null, 2));
      // const filePath = `./s/${service_id}_${service_name.split("-")[0]}_${
      //   service_name.split("-")[1].split("|")[0]
      // }.json`;
      // fs.writeFileSync(filePath, JSON.stringify(document, null, 2), "utf-8");
      // console.log(`Document saved as JSON file: ${filePath}`);

      // Check if the service_id already exists in the database
      const existingDoc = await collection.findOne({ service_id: service_id });

      if (existingDoc) {
        // Update the journeys array by adding new journeys only if the date is not already present
        const newJourneys = journeys.filter((journey) => {
          return !existingDoc.journeys.some((existingJourney) => {
            return existingJourney.date === journey.date;
          });
        });

        if (newJourneys.length > 0) {
          await collection.updateOne(
            { service_id: service_id },
            { $push: { journeys: { $each: newJourneys } } }
          );
          console.log(`Updated document for service_id: ${service_id}`);
        } else {
          console.log(`No new journeys to add for service_id: ${service_id}`);
        }
      } else {
        // Insert a new document
        await collection.insertOne(document);
        console.log(`Inserted new document for service_id: ${service_id}`);
      }

      docCount++;
    }

    console.log(`Total documents processed: ${docCount}`);
  } finally {
    await client.close();
  }
}

readCSV();
