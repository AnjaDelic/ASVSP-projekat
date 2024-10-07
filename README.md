# ASVSP (english version below)
Ovaj repozitorijum napravljen je s ciljem implementacije projekta iz predmeta Arhitektura sistema velikih skupova podataka, Fakultet tehnickih nauka, Novi Sad.

## Analiza saobraćajnih nesreća

Policijske uprave širom SAD-a poseduju više sistema koji ih izvešavaju o stanju u njihovom gradu
Osnovna ideja je, kombinacijom istorijske i obrade u realnom vremenu, pružiti policiji Čikaga jedan sistem koji prikazuje sveobuhvatnu sliku o stanju saobraćajnih nesreća duž SAD, a potom i u njihovom gradu kako bi na adekvatan način, i što efikasnije, raspodelili resurse i reagovali.
Za paketnu obradu odabran je skup o saobraćajnim nesrećama u SAD-u sa linka: https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents/data
Za obradu u realnom vremenu odabran je skup o saobraćajnim nesrećama u Čikagu: https://data.cityofchicago.org/Transportation/Traffic-Crashes-Crashes/85ca-t3if

## Skup podatka za paketnu obradu
Sadrži istorijske podatke o saobraćajnim nesrećama u SAD od februara 2016. do marta 2023. godine. 
-   **Veličina skupa podataka:** 3GB
-   **Broj kolona:** 46
-   **Broj redova:** 3 000 000

### Obeležja
S obzirom da ima 46 obeležja, navešće se samo najvažnija.
Tipovi obeležja koji se pojavljuju u bazi su: String (16), Decimal (10),  DateTime (2), Boolean (13) i Other (5) 

- ID - identifikaciono obeležje
- SEVERITY - ozbiljnost, uticaj, nesreće, broj od 1 do 4 (najozbiljnija) 
- STATE, CITY, COUNTY ,STREET, ZIPCODE,... - obeležja koja opisuju adresu nesreće
- CRASH_DATE, CRASH_HOUR, TIMEZONE... - obeležja koja opisuju datum i vreme nesreće
- WEATHER_CONDITION, TEMPERATURE(F), VISIBILITY(MI),  SUNRISE_SUNSET ... - obeležja koja se odnose na vremenske uslove u trenutku nesreće
- BUMP, CROSSING, JUNCTION, NO_EXIT, ROUNDABOUT, ... - obeležja koja se odnose na okolinu nesreće 

### Upiti
1. U kojih 5 država su najučestalije saobraćajne nesreće, koliko puta su zabeležene u svakoj od tih država i koji je to procenat od ukupnog broja zabeleženih nesreća?
2. Za svaku državu izlistati prosečan broj nezgoda za svaku godinu i za svaki mesec u toj godini.
3. Prikazati državu sa najviše nezgoda koje su imale značajan uticaj na okolinu, a desile su se tokom dana i kada je brzina vetra bila veća od prosečne. 
4. Izlistati nezgode koje su se dogodile u blizini saobraćajnih znakova, imale su uticaj stepena 2 na saobraćaj i desile su se tokom noći i slabe vidljivosti.
5. Izlistati nezgode čija je pojava izazvala kolonu od 5 do 7 kilometara, u blizini ima pešački prelaz i raskrsnicu i sortirati ih prema datumu opadajuće.
6. Izlistati gradove s zabeleženim brojem nezgoda i dodati kolonu koja odredjuje da li je taj broj manji ili veci i jednak od prosečnog broja zabeleženih nesreća. Dodatno, izračunati prosečnu temperaturu u stepenima celzijusa u svakom gradu. 
7. Prikazati 15 gradova koji imaju najveći broj nezgoda po uglavnom oblačnom vremenu i kada je zabeležena vlažnost veća od prosečne. 
8. Za 2022. godinu prikazati, za svaki grad prosečan broj nezgoda po danima i satima. 
9. Prikazati grad u kojem se najviše nezgoda dogodilo između 2017. i 2022. godine i koji je to udeo od ukupnog broja nezgoda, izraziti u procentima. 
10. Izlistati 5 gradova u kojima je najvise zabeleženih nezgoda blizu pešačkog prelaza i znaka stop. Za svaki od 5 gradova izlistati 3 najčešćih vremenskih uslova.
11. Za grad Čikago prikazati ukupan i prosečan broj nezgoda za svaki mesec.
12. Za grad Čikago prikazati ukupan i prosečan broj nezgoda po danima i satima.
13. Za grad Čikago izlistati vremenske uslove i doba dana u kojima se najčešće dogadjaju nesreće.

## Skup podataka za obradu u realnom vremenu
Sadrži istorijske podatke o saobraćajnim nesrećama u Čikagu od februara 2015. godine. 
-   **Veličina skupa podataka:** 430MB
-   **Broj kolona:** 48
-   **Broj redova:** 784 000

### Obeležja
S obzirom da ima 48 obeležja, navešće se samo najvažnija.
Tipovi obeležja koji se pojavljuju u bazi su: String (20), Integer (15),  DateTime (2), Boolean (9) i Other (2) 
- CRASH_RECORD_ID - identifikaciono obeležje
- LONGITUDE, LATITUDE, LOCATION, STREET_NAME,... - obeležja koja opisuju adresu nesreće
- CRASH_DATE, CRASH_HOUR,... - obeležja koja opisuju datum nesreće
- WEATHER_CONDITION, LIGHTING_CONDITION,... - obeležja koja se odnose na vremenske uslove u trenutku nesreće
- ROADWAY_SURFACE_COND, ROAD_DEFECT, WORK_ZONE_I, WORKERS_PRESENT_I,... - obeležja koja se odnose na okolinu nesreće
- MOST_SEVERE_INJURY, INJURIES_TOTAL, INJURIES_FATAL,INJURIES_UNKNOWN,... - obeležja koja se odnose na broj povreda zadobijenih u nesreći
- DAMAGE - procenjena šteta
- CRASH_TYPE - tip nesreće može biti ili povreda / potrebna vuča zbog sudara ili bez povreda / u stanju da se odveze
- PRIM_CONTRIBUTORY_CAUSE, SEC_CONTRIBUTORY_CAUSE - obeležja koja se odnose na uzroke nesreće
- INTERSECTION_RELATED_I, HIT_AND_RUN_I, DOORING_I,... - detalji nesreće   
- BEAT_OF_OCCURRENCE - bit, deo grada namenjen  za 1 policijsku patrolu, u kojem se dogodila nesreća
- PHOTOS_TAKEN_I, STATEMENTS_TAKEN_I - obeležja koja se odnose na to da li je policijski službenik izvršio svoje dužnosti

### Upiti
1. U zavisnosti od glavnog uzroka nesreće prikazati broj nesreća u prethodnih 5min, s pomeranjem prozora od 1min.
2. Prikazati broj fatalnih i nefatalnih nesreća u prethodne 3sekunde, s prozorom pomeranja od 1 sekund.
3. Ispisati da li su uočene nesreće u prethodnih 5min iznad ili ispod proseka. 
4. Prikazati koliki % od ukupnog broja uočenih nesreća se dogodilo u prethodnih 5 min.
5. Prikazati najučestaliji vremenski uslov istorijski, kao i vremenske uslove i broj nesreća po istima u prethodnih 15min.


# ASVSP
This repository was created with the aim of implementing a project for the course on Large Data Set System Architecture, Faculty of Technical Sciences, Novi Sad.

## Traffic Accident Analysis

Police departments across the USA possess multiple systems that inform them about the situation in their cities.  
The basic idea is to provide the Chicago police with a single system that combines historical data and real-time processing to present a comprehensive picture of traffic accidents across the USA and, subsequently, in their city. This will enable them to allocate resources adequately and respond more efficiently.  
For batch processing, a dataset on traffic accidents in the USA was selected from the link: [US Accidents Dataset](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents/data).  
For real-time processing, a dataset on traffic accidents in Chicago was selected: [Chicago Traffic Crashes Dataset](https://data.cityofchicago.org/Transportation/Traffic-Crashes-Crashes/85ca-t3if).

## Dataset for Batch Processing
Contains historical data on traffic accidents in the USA from February 2016 to March 2023. 
- **Dataset Size:** 3GB
- **Number of Columns:** 46
- **Number of Rows:** 3,000,000

### Features
Given that there are 46 features, only the most important will be listed.  
The types of features that appear in the database are: String (16), Decimal (10), DateTime (2), Boolean (13), and Other (5). 

- **ID** - identification feature  
- **SEVERITY** - severity, impact, accident, a number from 1 to 4 (most severe)  
- **STATE, CITY, COUNTY, STREET, ZIPCODE, ...** - features that describe the accident address  
- **CRASH_DATE, CRASH_HOUR, TIMEZONE, ...** - features that describe the date and time of the accident  
- **WEATHER_CONDITION, TEMPERATURE (F), VISIBILITY (MI), SUNRISE_SUNSET, ...** - features related to the weather conditions at the time of the accident  
- **BUMP, CROSSING, JUNCTION, NO_EXIT, ROUNDABOUT, ...** - features related to the accident environment  

### Queries
1. In which 5 states are traffic accidents most frequent, how many times were they recorded in each of those states, and what percentage does that represent of the total number of recorded accidents?
2. For each state, list the average number of accidents for each year and for each month in that year.
3. Display the state with the most accidents that had a significant impact on the environment, occurred during the day, and when wind speed was above average.  
4. List accidents that occurred near traffic signs, had a severity impact of 2 on traffic, and happened at night in low visibility.  
5. List accidents that caused a traffic jam of 5 to 7 kilometers, nearby there is a pedestrian crossing and an intersection, and sort them by date in descending order.  
6. List cities with recorded numbers of accidents and add a column that determines whether that number is below, above, or equal to the average number of recorded accidents. Additionally, calculate the average temperature in degrees Celsius for each city.  
7. Display the 15 cities with the highest number of accidents during mostly cloudy weather and when humidity was above average.  
8. For the year 2022, show the average number of accidents per day.  
9. Show the city with the highest number of accidents that occurred between 2017 and 2022 and what percentage that represents of the total number of accidents.  
10. List 5 cities where the highest number of accidents were recorded near a pedestrian crossing and a stop sign. For each of the 5 cities, list the 3 most common weather conditions.  
11. For Chicago, display the total and average number of accidents for each month.  
12. For Chicago, display the total and average number of accidents by day and hour.  
13. For Chicago, list the weather conditions and times of day in which accidents most frequently occur.

## Dataset for Real-Time Processing
Contains historical data on traffic accidents in Chicago from February 2015. 
- **Dataset Size:** 430MB
- **Number of Columns:** 48
- **Number of Rows:** 784,000

### Features
Given that there are 48 features, only the most important will be listed.  
The types of features that appear in the database are: String (20), Integer (15), DateTime (2), Boolean (9), and Other (2). 
- **CRASH_RECORD_ID** - identification feature  
- **LONGITUDE, LATITUDE, LOCATION, STREET_NAME, ...** - features that describe the accident address  
- **CRASH_DATE, CRASH_HOUR, ...** - features that describe the date of the accident  
- **WEATHER_CONDITION, LIGHTING_CONDITION, ...** - features related to the weather conditions at the time of the accident  
- **ROADWAY_SURFACE_COND, ROAD_DEFECT, WORK_ZONE_I, WORKERS_PRESENT_I, ...** - features related to the accident environment  
- **MOST_SEVERE_INJURY, INJURIES_TOTAL, INJURIES_FATAL, INJURIES_UNKNOWN, ...** - features related to the number of injuries sustained in the accident  
- **DAMAGE** - estimated damage  
- **CRASH_TYPE** - type of accident, which can be injury / requiring towing due to a collision or without injuries / able to drive away  
- **PRIM_CONTRIBUTORY_CAUSE, SEC_CONTRIBUTORY_CAUSE** - features related to the causes of the accident  
- **INTERSECTION_RELATED_I, HIT_AND_RUN_I, DOORING_I, ...** - details of the accident  
- **BEAT_OF_OCCURRENCE** - beat, area of the city designated for 1 police patrol, where the accident occurred  
- **PHOTOS_TAKEN_I, STATEMENTS_TAKEN_I** - features related to whether the police officer performed their duties  

### Queries
1. Depending on the main cause of the accident, display the number of accidents in the previous 5 minutes, with a sliding window of 1 minute.
2. Display the number of fatal and non-fatal accidents in the previous 3 seconds, with a sliding window of 1 second.
3. Output whether the observed accidents in the previous 5 minutes are above or below average. 
4. Show what percentage of the total observed accidents occurred in the previous 5 minutes.
5. Display the most common weather condition historically, as well as the weather conditions and the number of accidents for those conditions in the previous 15 minutes.



