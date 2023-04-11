db.drivers.find()
// Year  Driver
db.races.find()
// Date  P1


db.races.aggregate([
    {$lookup:
        { from: 'drivers',
        localField: 'Date',
        foreignField: 'Year',
        as : 'drivers'
        }
        
    },
    {$match:{'drivers.Driver':'Fernando Alonso', Location:'Spain', 'drivers.Car':'Renault'}},
     
])

// 1* Quien ha ganado mas carreras a lo largo de la historia

var fase1= { $group: { "_id": "$P1", "cuenta": { $sum: 1 } } }
var fase2= { $sort: { "cuenta" : -1 } }
var etapas= [ fase1, fase2 ]
db.races.aggregate( etapas )

db.drivers.distinct("Driver")

// 1** Quien tiene mas mundiales
db.drivers.aggregate([
    { $match: { Pos: "1" } },
    { $group: { _id: "$Driver", total: { $sum: 1 } } },
    { $sort: { total: -1 } }])

 
// 1. Numero de carreras ganadas por Lewis Hamilton

db..find({"P1":"Lewis Hamilton"}).count()

    // 1.1 Carreras que ha ganado Hamilton
        var filtro1 = { "P1": "Lewis Hamilton" }
        var filtro2 = { "Location": 1,"P1":1}
        db.races.find(filtro1,filtro2)



// 2. Total de victorias en cada circuito que ha ganado  Hamilton
db.races.aggregate([
    { $match: { P1: "Lewis Hamilton" } },
    { $group: { _id: "$Location", total: { $sum: 1 } } },
    { $sort: { total: -1 } }
])
        //2.1 Que grandes premios ganó Hamilton en 2019
         var filtro1 = { P1: "Lewis Hamilton", "Date":"2007" }
        var filtro2 = { "Location": 1,"Date":1,"P1":1}
        db.races.find(filtro1,filtro2)
        
        //2.2 Entre Alonso y Hamilton en 2007,¿Quien acabo ese mundial con más puntos?
         var filtro1 =  {$or: [ { "Driver": "Fernando Alonso" }, { "Driver": "Lewis Hamilton" } ],"Year":"2007"}
         var filtro2 = { "Driver": 1,"Year":1,"PTS":1}
         db.drivers.find(filtro1,filtro2)
        

// 3. Cuantas veces gano el mundial Fernando Alonso
db.drivers.find({"Driver":"Fernando Alonso","Pos":"1"}).count()

  // 3.1 En que años y con que coche gano el mundial Rosberg
    var query = { "Driver": "Nico Rosberg", "Pos":"1" }
    var project1 = { "Pos":1,"Year": 1,"Car":1, }
    db.drivers.find(query,project1)

// 4. La nacionalidad que más predomina en la hisotira

db.drivers.aggregate([
{ $group: { _id: "$Nationality", total: { $sum: 1 } } },
{ $sort: { total: -1 } }
])

    
    // 4.1 Nombre de todos los pilotos con nacionalidad ESP por año
            var query = { "Nationality": "ESP" }
            var project1 = { "Driver":1,"Short":1, "Pos": 1,"Year":1,"Car":1 }
            db.drivers.find(query,project1)

// 5. Que coche ha ganado mas titulos mundiales en toda la historia
db.drivers.aggregate([
    { $match: { Pos: "1" } },
    { $group: { _id: "$Car", total: { $sum: 1 } } },
    { $sort: { total: -1 } }])
    
    // 5.1  Quien ganó con Red Bull Racing Renault
        var query = { "Car": "Ferrari", "Pos":"1" }
        var project1 = { "Pos":1,"Year": 1,"Car":1,"Driver":1 }
        db.drivers.find(query,project1)

// 6. Que coche ha ganado mas títulos mundiales en los últimos 20 años
db.drivers.aggregate([
    { $match: { Pos: "1" ,Year:{$gt:"2000"}} },
    { $group: { _id: "$Car", total: { $sum: 1 } } },
    { $sort: { total: -1 } }])
    
        // 6.1 Quien ganó con ferrari en los útlimos 20 años
            var query = { "Car": "Ferrari", "Pos":"1","Year":{$gt:"2000"} }
            var project1 = { "Pos":1,"Year": 1,"Car":1,"Driver":1 }
            db.drivers.find(query,project1)
    
            
// 7. Buscar todos los pilotos que hayan corrido coches con motor Mercedes
// mostrar tambien poscion, año y marca del coche completa
busqueda= { "Car": { $regex: /Mercedes/ },"Year":{$gt:"2000"}}
var project1 = {"Driver":1, "Pos":1,"Year": 1,"Car":1 }
db.drivers.find(busqueda, project1)

db.drivers.aggregate(
    {$project: {result: { $convert: { input: Long("$Pos"), to: "int" } }}})
//8. Media de posiciones en el mundial de todos los pilotos desde el año 1990.
// Donde se muestra el total de mundiales disputados, la peor posicion del mundial, la mejor, y la media.
// He convertido los valores de Year y Pos de String -> Int

PosQtyConversionStage = {
   $addFields: {
         
      Pos_converted: { $convert:
         {
            input: "$Pos",
            to: "int",
            onError:{ $concat:
               [
                  "Could not convert ",
                  { $toString:"$qty" },
                  " to type integer."
               ]
            },
         onNull: Int32("0")
      } },
   }
};


YearQtyConversionStage1 = {
   $addFields: {
         
      Year_converted: { $convert:
         {
            input: "$Year",
            to: "int",
            onError:{ $concat:
               [
                  "Could not convert ",
                  { $toString:"$qty" },
                  " to type integer."
               ]
            },
         onNull: Int32("0")
      } },
   }
};

db.drivers.aggregate( [
   PosQtyConversionStage,
    YearQtyConversionStage1,
    //{ $match: { Year_converted:{$lte: 2007}} }
    { $group: { "_id": "$Driver", "conteo": { $sum: 1 }, "media": { $avg : "$Pos_converted" } "peor": { $max: "$Pos_converted" }, "mejor": { $min: "$Pos_converted" }} }
    { $sort: { "media": 1 } }
])
    
//9. Que pilotos  han estado en el top 5 
PosQtyConversionStage = {
   $addFields: {
         
      Pos_converted: { $convert:
         {
            input: "$Pos",
            to: "int",
            onError:{ $concat:
               [
                  "Could not convert ",
                  { $toString:"$qty" },
                  " to type integer."
               ]
            },
         onNull: Int32("0")
      } },
   }
};
YearQtyConversionStage1 = {
   $addFields: {
         
      Year_converted: { $convert:
         {
            input: "$Year",
            to: "int",
            onError:{ $concat:
               [
                  "Could not convert ",
                  { $toString:"$qty" },
                  " to type integer."
               ]
            },
         onNull: Int32("0")
      } },
   }
};
db.drivers.aggregate( [
   PosQtyConversionStage, YearQtyConversionStage1,
    { $match: { Pos_converted:{$lte: 3}, Year_converted:{$gt:1990}} }
   { $group: { "_id": "$Driver"} }
 
])

    
//10. Cuantos grandes premios se disputaron  en cada año (en cque año se corrieron mas GGPP)
db.races.aggregate( [
    {  $group: {  _id: "$Date", total: {  $sum: 1   }   } }
    {$sort: {total:-1}}  ])

     
    //10.2 Cuantas carreras se han corrido en 2019

        db.races.aggregate( [
        {$match:{"Date":"2019"}}
        {  $group: {  _id: "$Date", total: {  $sum: 1   }   } }])
     

//11. Concatenar nombre del piloto con su abreviación "Short"
var concatenar = { $concat: [ "$Driver", "-> ", "$Short"] }
var nombre = { $toUpper: concatenar }
var fase1 = { $project: { _id: 0, "Nombre" : nombre} }
var etapas = [ fase1 ]
db.drivers.aggregate( etapas )

//12 $lookUper : Uniendo las colecciones drivers y races
// En que posicion quedó Fernando Alonso en el GP de Turqía en 2007
db.drivers.aggregate([
    {$lookup:
        { from: 'races',
        localField: 'Year',
        foreignField: 'Date',
        as : 'races'
        }
            {$match:{Driver:'Fernando Alonso',Year:"2007"}
            
                
    },
     
])

//13 Quien gano el polémico GP de Humgría 2007?
// Se podría hacer desde la coleccion races, pero tambien nos interesaria saber el coche con el que corrio ese famoso GP y el
//resto de informacion personal que nos proporciona la coleccion drivers.
           
db.drivers.aggregate( [
   {
      $lookup:
         {
           from: "races",
           let:{driver_qty: "$Year",driver_item: "$Driver" },
           pipeline: [
              { $match: 
                
                 { $expr: 
                    { $and:
                       [
                          {$eq :["$Date", "$$driver_qty"]},
                          {$eq :["$Date", "2007"]},
                          {$eq: [ "$P1",  "$$driver_item" ] },
                          {$eq :["$Location", "Hungary"]},
                          
                          
                       ]
                    }
                    
                 }
            
              },
              
              { $project: { driver_qty: 0, _id: 0,}
            
           ],
           as: "data"
    }
             
         },
         {$match: { $expr: { $eq: [{ $size: "$data" }, 1]}  } },
    
    ] )       
           
           
           
           
