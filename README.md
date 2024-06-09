# ASVSP
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
5. Izlistati nezgode čija je pojava izazvala kolonu od 5 do 7 milja, u blizini ima pešački prelaz i raskrsnicu i sortirati ih prema datumu opadajuće.
6. Izlistati gradove u kojima je zabeležen broj nezgoda manji od prosečnog broja zabeleženih nesreća i u njima izračunati prosečnu temperaturu u stepenima celzijusa. 
7. Prikazati 15 gradova koji imaju najveći broj nezgoda po uglavnom oblačnom vremenu i kada je zabeležena vlažnost veća od prosečne. 
8. Za 2022. godinu prikazati, za svaki grad prosečan broj nezgoda po danima i satima. 
9. Prikazati grad u kojem se najviše nezgoda dogodilo između 2017. i 2022. godine i koji je to udeo od ukupnog broja nezgoda, izraziti u procentima. 
10. Izlistati 5 gradova u kojima je najvise zabeleženih nezgoda blizu pešačkog prelaza i znaka stop. Za svaki od 5 gradova izlistati 3 najčešćih vremenskih uslova.
11. Za grad Čikago prikazati prosečan broj nezgoda za svaki mesec.
12. Za grad Čikago prikazati prosečan broj prosečan broj nezgoda po danima i satima.
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
1. Izlistati 15 bitova u kojima je toga dana bilo najviše nezgoda, sortirati ih u opadajućem redosledu zabeleženih nezgoda i prikazati koji je to udeo od ukupnog zabeleženog broja nezgoda toga dana.
2. U zavisnosti od glavnog uzroka nezgode, izlistati ukupan broj fatalnih i nefatalnih nezgoda sortiranih po ukupnoj šteti za taj uzrok opadajuće.
3. Za krug 3 kilometra od centra, izlistati 3 tipa nesreća koji su se najviše puta dogodili toga dana i sortirati ih prema rastućem broju uočenih zločina. 
4. Izlistati broj nesreća za svaki sat u tom danu. 
5. Izlistati vremenske uslove i doba dana zabeleženih nezgoda i sortirati ih opadajuće.
6. Izlistati nezgode koje su se desile danju a gde je krivac pobegao, šteta je preko 1500 dolara i put je bio mokar. Sortirati po broju 





