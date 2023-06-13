const {MongoClient}= require('mongodb');
const { faker } = require('@faker-js/faker');
require('dotenv').config();
const URI = process.env.URI;

async function Pedidos(){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db('SoftDCano').createCollection("Pedidos",{
            validator:{
                $jsonSchema:{
                    bsonType: 'object',
                    title:'validacionPedidos',
                    required:['idPedido', 'dni', 'idProducto', 'estadoPedido', 'fechaPedido', 'tipoPagoPedido', 'totalPedido'],
                    properties:{
                        idPedido:{
                            bsonType: 'int'
                        },
                        dni: {
                            bsonType: 'int'
                        },
                        idProducto:{
                            bsonType: 'int'
                        },
                        estadoPedido: {
                            bsonType: 'string'
                        },
                        fechaPedido: {
                            bsonType: 'date'
                        },
                        tipoPagoPedido: {
                            bsonType: 'string'
                        },
                        totalPedido: {
                            bsonType: 'int'
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
// Pedidos();

async function PoblarPedidos(RegistrosPedidos){

    const Client = new MongoClient(URI)

    try {
        await Client.connect();
        const Pedidos = await Client.db("SoftDCano").collection("Pedidos").find({}).toArray();
        const DatosComprador = await Client.db("SoftDCano").collection("DatosComprador").find({}).project({dni:true,_id:false}).toArray();
        const Productos = await Client.db("SoftDCano").collection("Productos").find({}).project({idProducto:true,_id:false}).toArray();
        const Datos = [];
        for (let i=0; i<RegistrosPedidos;i++){
            const DatosInsertar = {
                idPedido: Pedidos.length+i,
                dni: faker.helpers.arrayElement(DatosComprador).dni,
                idProducto: faker.helpers.arrayElement(Productos).idProducto,
                estadoPedido: faker.helpers.arrayElement(["Confirmar","Cancelado", "Enviado", "Entregado"]),
                fechaPedido: new Date(faker.date.recent()),
                tipoPagoPedido: faker.helpers.arrayElement(["Efectivo","Transferencia"]),
                totalPedido: faker.number.int({min:10000, max:100000}),
            }
            Datos.push(DatosInsertar);
            console.log(`Se han insertado: ${Datos.length} datos`)
        }
        const Result= await Client.db('SoftDCano').collection('Pedidos').insertMany(Datos)
    }catch(e){
        console.log(e);
    }finally{
        await Client.close();
    }


}
// PoblarPedidos(2000);

//CRUD
//FIND
async function FindOnePedido(estaPedido){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").findOne({estadoPedido: estaPedido});
        if(result){
            console.log(`Se encontro un pedido con el siguiente estado: ${estaPedido}`);
            console.log(result);
        }else{
            console.log(`No se encontro un pedido con el siguiente estado: ${estaPedido}`);
        }
    }catch(e){
        console.log(e);
    }finally{
        await Client.close();
    }
}

FindOnePedido("Entregado");

async function FindPedido(estaPedido){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").find({estadoPedido: estaPedido}).project({idPedido: true, estadoPedido: true, tipoPagoPedido: true}).sort({tipoPagoPedido: 1}).limit(10).toArray();
        if(result.length > 0){
            console.log(`Se encontro un pedido con el siguiente estado: ${estaPedido}`);
            console.log(result);
        }else{
            console.log(`No se encontro un pedido con el siguiente estado: ${estaPedido}`);
        }
    }catch(e){
        console.log(e);
    }finally{
        await Client.close();
    }
}

// FindPedido("Confirmar");

//CREATE
async function insertOnePedido(nuevoPedido){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").insertOne(nuevoPedido);
        console.log(`Se creo un nuevo pedido con el siguiente id: ${result.insertedId}`);
        console.log(nuevoPedido);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// insertOnePedido({
//     idPedido: 2000,
//     dni: 718475348,
//     idProducto: 88,
//     estadoPedido: "Confirmar",
//     fechaPedido: new Date("2023-05-21T05:27:08.754+00:00"),
//     tipoPagoPedido: "Transferencia",
//     totalPedido: 25500,
    
// });

// insertOnePedido({
//     idPedido: 2004,
//     dni: 762540687,
//     idProducto: 23,
//     estadoPedido: "Entregado",
//     fechaPedido: new Date("2023-05-23T05:27:08.754+00:00"),
//     tipoPagoPedido: "Efectivo",
//     totalPedido: 11400,
//     comentarios: "La ropa es de muy buena calidad",
//     calificaciones: [5, 5, 5, 4],
// });

async function insertManyPedido(nuevoPedido){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").insertMany(nuevoPedido);
        console.log(`Se creo ${result.insertedCount} nuevos pedidos`);
        console.log(nuevoPedido);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

const nuevoPedido = [
    {
        idPedido: 2001,
        dni: 718475348,
        idProducto: 88,
        estadoPedido: "Confirmar",
        fechaPedido: new Date("2023-05-21T05:27:08.754+00:00"),
        tipoPagoPedido: "Transferencia",
        totalPedido: 25500,
    },
    {
        idPedido: 2002,
        dni: 991884598,
        idProducto: 20,
        estadoPedido: "Entregado",
        fechaPedido: new Date("2023-05-21T05:27:08.754+00:00"),
        tipoPagoPedido: "Efectivo",
        totalPedido: 10500,
    },
    {
        idPedido: 2003,
        dni: 563524623,
        idProducto: 56,
        estadoPedido: "Cancelado",
        fechaPedido: new Date("2023-05-22T05:27:08.754+00:00"),
        tipoPagoPedido: "Transferencia",
        totalPedido: 55000,
    }
  ];
  
// insertManyPedido(nuevoPedido);

//UPDATE
async function updateOnePedido(Pedido, atributoCambio){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").updateOne
        ({idPedido: Pedido},{$set:{estadoPedido: atributoCambio}});
        console.log(`${result.matchedCount} documento cumple con el criterio de busqueda`);
        console.log(`${result.modifiedCount} documento fue actualizado`);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// updateOnePedido(2002, "Cancelado");

async function updateManyPedido(estado, atributoCambio){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").updateMany
        ({estadoPedido: estado},{$set:{estadoPedido: atributoCambio}});
        console.log(`${result.matchedCount} documentos cumplen con el criterio de busqueda`);
        console.log(`${result.modifiedCount} documentos fueron actualizados`);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// updateManyPedido("Confirmar", "Enviado");

async function updateManyUpsertPedido(estado, atributoCambio){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").updateMany(
            {comentarios:{$exists: false}},
            {$set:{comentarios: "La ropa es muy bonita"}},
            {$upsert:true});
        console.log(`${result.matchedCount} documentos cumplen con el criterio de busqueda`);
        console.log(`${result.modifiedCount} documentos fueron actualizados`);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// updateManyUpsertPedido();

//DELETE
async function deleteOnePedido(eliminarPedido){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").deleteOne(eliminarPedido);
        console.log(`${result.deletedCount} pedido eliminado`);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// deleteOnePedido({idPedido: 2003});

async function deleteManyPedido(eliminarPedidos){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").deleteMany(eliminarPedidos);
        console.log(`${result.deletedCount} pedidos eliminados`);
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// deleteManyPedido({estadoPedido: "Cancelado"});

async function dropCollectionPedido(eliminarColeccion){
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

// dropCollection("Pedidos");

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
async function aggregatePedidos() {
    const Client = new MongoClient(URI);
  
    try {
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").aggregate([
            {
                $lookup: {
                    from: "DatosComprador",
                    localField: "dni",
                    foreignField: "dni",
                    as: "datosComprador"
                }
            },{
                $sort: {
                    idPedido: -1,
                }
            },{
                $limit: 5,
            },{
                $project: {
                    idPedido: true,  
                    fechaPedido: true, 
                    tipoPagoPedido: true,
                    datosComprador: true
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
async function aggregate2Pedidos() {
    const Client = new MongoClient(URI);
  
    try {
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").aggregate([
            {
                $lookup: {
                    from: "Productos",
                    localField: "idProducto",
                    foreignField: "idProducto",
                    as: "productos"
                }
            },{
                $unwind: "$calificaciones"
            },{
                $project: {
                    idPedido: true, 
                    estadoPedido: true, 
                    tipoPagoPedido: true,
                    productos: true,
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
  
// aggregate2Pedidos();