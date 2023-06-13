const {MongoClient}= require('mongodb');
const { faker } = require('@faker-js/faker');
require('dotenv').config();
const URI = process.env.URI;

async function Ventas(){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db('SoftDCano').createCollection("Ventas",{
            validator:{
                $jsonSchema:{
                    bsonType: 'object',
                    title:'validacionVentas',
                    required:['idVenta', 'idUsuario', 'idPedido','dni','fechaVenta','estadoVenta', 'tipoPagoVenta', 'totalVenta', 'comprobante'],
                    properties:{
                        idVenta:{
                            bsonType: 'int'
                        },
                        idUsuario:{
                            bsonType: 'int'
                        },
                        idPedido:{
                            bsonType: 'int'
                        },
                        dni:{
                            bsonType: 'int'
                        },
                        fechaVenta: {
                            bsonType: 'date'
                        },
                        estadoVenta: {
                            bsonType: 'string'
                        },
                        tipoPagoVenta: {
                            bsonType: 'string'
                        },
                        totalVenta: {
                            bsonType: 'int'
                        },
                        comprobante: {
                            bsonType: 'string'
                        }
                    }
                }
            }
        })
        if (result){
            console.log("Base de datos creada correctamente");
        }else{
            console.log("No se ha creado la base de datos");
        }
    }catch(e){
        console.log(e);
    }finally{
        await Client.close();
    }
}
// Ventas();

async function PoblarVentas(RegistrosVentas){

    const Client = new MongoClient(URI)

    try {
        await Client.connect();
        const Ventas = await Client.db("SoftDCano").collection("Ventas").find({}).toArray();
        const Usuario = await Client.db("SoftDCano").collection("Usuario").find({}).project({idUsuario:true,_id:false}).toArray();
        const Pedidos = await Client.db("SoftDCano").collection("Pedidos").find({}).project({idPedido:true,_id:false}).toArray();
        const DatosComprador = await Client.db("SoftDCano").collection("DatosComprador").find({}).project({dni:true,_id:false}).toArray();
        const Datos = [];
        for (let i=0; i<RegistrosVentas;i++){
            const DatosInsertar = {
                idVenta: Ventas.length+i,
                idUsuario: faker.helpers.arrayElement(Usuario).idUsuario,
                idPedido: faker.helpers.arrayElement(Pedidos).idPedido,
                dni: faker.helpers.arrayElement(DatosComprador).dni,
                fechaVenta: new Date(faker.date.recent()),
                estadoVenta: faker.helpers.arrayElement(["Cancelada", "En proceso", "Completada"]),
                tipoPagoVenta: faker.helpers.arrayElement(["Efectivo","Transferencia"]),
                totalVenta: faker.number.int({min:10000, max:100000}), 
                comprobante: faker.image.url(),

            }
            Datos.push(DatosInsertar);
            console.log(`Se han insertado: ${Datos.length} datos`)
        }
        const Result= await Client.db('SoftDCano').collection('Ventas').insertMany(Datos)
        
    }catch(e){
        console.log(e);
    }finally{
        await Client.close();
    }


}
// PoblarVentas(2000);

//CRUD
//FIND

async function FindOneVenta(IdVenta){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").findOne({idVenta: IdVenta});
        if(result){
            console.log(`Se encontro una venta con el siguiente Id: ${IdVenta}`);
            console.log(result);
        }else{
            console.log(`No se encontro una venta con el siguiente Id: ${IdVenta}`);
        }
    }catch(e){
        console.log(e);
    }finally{
        await Client.close();
    }
}

// FindOneVenta(88);

async function FindVenta(PagoVenta){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").find({tipoPagoVenta: PagoVenta}).project({idVenta: true, idUsuario: true, dni: true, tipoPagoVenta: true, totalVenta: true}).sort({totalVenta: -1}).limit(10).toArray();
        if(result.length > 0){
            console.log(`Se encontraron ventas con el siguiente tipo de pago: ${PagoVenta}`);
            console.log(result);
        }else{
            console.log(`No se encontraron ventas con el siguiente tipo de pago: ${PagoVenta}`);
        }
    }catch(e){
        console.log(e);
    }finally{
        await Client.close();
    }
}

// FindVenta("Efectivo");

//CREATE
async function insertOneVenta(nuevaVenta){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").insertOne(nuevaVenta);
        console.log(`Se creo una nueva venta con el siguiente id: ${result.insertedId}`);
        console.log(nuevaVenta);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}
// insertOneVenta({
//     idVenta: 2000,
//     idUsuario: 786897402,
//     idPedido: 45,
//     dni: 507540174,
//     fechaVenta: new Date("2023-05-23T05:27:15.754+00:00"),
//     estadoVenta: "Completada",
//     tipoPagoVenta: "Transferencia",
//     totalVenta: 16000,
//     comprobante: "https://loremflickr.com/640/480?lock=2212244194590720",
// });

// insertOneVenta({
//     idVenta: 2005,
//     idUsuario: 114719248,
//     idPedido: 145,
//     dni: 121066255,
//     fechaVenta: new Date("2023-05-23T05:27:15.754+00:00"),
//     estadoVenta: "Completada",
//     tipoPagoVenta: "Efectivo",
//     totalVenta: 88000,
//     comprobante: "https://loremflickr.com/640/480?lock=2212244194590720",
//     comentarios: "Me gusto la atencion",
//     calificaciones: [5, 4, 3, 2],
// });

async function insertManyVenta(nuevaVenta){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").insertMany(nuevaVenta);
        console.log(`Se creo ${result.insertedCount} nuevas ventas`);
        console.log(nuevaVenta);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

const nuevaVenta = [
    {
        idVenta: 2001,
        idUsuario: 428178174,
        idPedido: 165,
        dni: 520463743,
        fechaVenta: new Date("2023-05-22T05:14:46.754+00:00"),
        estadoVenta: "Completada",
        tipoPagoVenta: "Transferencia",
        totalVenta: 55800,
        comprobante: "https://loremflickr.com/640/480?lock=6742838438002688",
    },
    {
        idVenta: 2002,
        idUsuario: 429084990,
        idPedido: 508,
        dni: 644276661,
        fechaVenta: new Date("2023-05-22T05:15:46.754+00:00"),
        estadoVenta: "En proceso",
        tipoPagoVenta: "Transferencia",
        totalVenta: 22300,
        comprobante: "https://picsum.photos/seed/6Gs6IcTM1k/640/480",
    },
    {
        idVenta: 2003,
        idUsuario: 260737121,
        idPedido: 789,
        dni: 971242952,
        fechaVenta: new Date("2023-05-22T05:16:10.754+00:00"),
        estadoVenta: "En proceso",
        tipoPagoVenta: "Efectivo",
        totalVenta: 12500,
        comprobante: "https://picsum.photos/seed/1my3v/640/480",
    },
    {
        idVenta: 2004,
        idUsuario: 260012975,
        idPedido: 453,
        dni: 618301363,
        fechaVenta: new Date("2023-05-22T05:17:15.754+00:00"),
        estadoVenta: "Cancelada",
        tipoPagoVenta: "Efectivo",
        totalVenta: 26700,
        comprobante: "https://loremflickr.com/640/480?lock=2693420562776064",
    }
  ];
  
// insertManyVenta(nuevaVenta);

//UPDATE
async function updateOneVenta(IdVenta, atributoCambio){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").updateOne
        ({idVenta: IdVenta},{$set:{estadoVenta: atributoCambio}});
        console.log(`${result.matchedCount} documento cumple con el criterio de busqueda`);
        console.log(`${result.modifiedCount} documento fue actualizado`);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// updateOneVenta(2004, "Completada");

async function updateManyVenta(estaVenta, atributoCambio){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").updateMany
        ({estadoVenta: estaVenta},{$set:{estadoVenta: atributoCambio}});
        console.log(`${result.matchedCount} documentos cumplen con el criterio de busqueda`);
        console.log(`${result.modifiedCount} documentos fueron actualizados`);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// updateManyVenta("En proceso", "Cancelada");

async function updateManyUpsertVenta(){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").updateMany(
            {comentarios:{$exists: false}},
            {$set:{comentarios: "Excelente atención"}},
            {$upsert:true});
        console.log(`${result.matchedCount} documentos cumplen con el criterio de busqueda`);
        console.log(`${result.modifiedCount} documentos fueron actualizados`);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// updateManyUpsertVenta();

//DELETE
async function deleteOneVenta(eliminarVenta){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").deleteOne(eliminarVenta);
        console.log(`${result.deletedCount} venta eliminada`);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// deleteOneVenta({idVenta: 2002});

async function deleteManyVenta(eliminarVentas){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").deleteMany(eliminarVentas);
        console.log(`${result.deletedCount} ventas eliminadas`);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// deleteManyVenta({estadoVenta: "Cancelada"});

async function dropCollectionVenta(eliminarColeccion){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection(eliminarColeccion).drop();
        console.log(`La colección ${eliminarColeccion} ha sido eliminada`);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// dropCollectionVenta("Ventas");

async function dropDatabase(eliminarBasedeDatos){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db(eliminarBasedeDatos).dropDatabase();
        console.log(`La base de datos ${eliminarBasedeDatos} ha sido eliminada`);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// dropDatabase("SoftDCano");

//Pipelines, lookup
async function aggregateVentas() {
    const Client = new MongoClient(URI);
  
    try {
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").aggregate([
            {
                $lookup: {
                    from: "Pedidos",
                    localField: "idPedido",
                    foreignField: "idPedido",
                    as: "pedidos"
                }
            },{
                $sort: {
                    idVenta: 1,
                }
            },{
                $limit: 4,
            },{
                $project: {
                    idVenta: true,
                    idPedido: true,  
                    dni: true,
                    tipoPagoVenta: true,
                    pedidos: true
                }
            }
        ]).toArray();
        console.log("Consulta exitosa");
    } catch (e) {
      console.log(e);
    } finally {
      await Client.close();
    }
}
  
// aggregatePedidos();

//Pipelines, lookup y unwind
async function aggregate2Ventas() {
    const Client = new MongoClient(URI);
  
    try {
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").aggregate([
            {
                $lookup: {
                    from: "Usuario",
                    localField: "idUsuario",
                    foreignField: "idUsuario",
                    as: "usuario"
                }
            },{
                $unwind: "$calificaciones" 
            },{
                $project: {
                    idVenta: true,
                    idUsuario: true,
                    dni:true,
                    estadoVenta: true, 
                    comprobante: true,
                    usuario: true,
                    calificaciones: true,
                }
            }
        ]).toArray();
        console.log("Consulta exitosa");
    } catch (e) {
      console.log(e);
    } finally {
      await Client.close();
    }
}