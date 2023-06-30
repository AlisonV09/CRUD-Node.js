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
        const Result= await Client.db('SoftDCano').collection('Pedidos').insertMany(Datos);
        if (Result.insertedCount > 0) {
            console.log(`Se han insertado ${Result.insertedCount} documentos correctamente`);
        } else {
            console.log("No se han insertado documentos");
        }
    }catch(e){
        console.log(e);
    }finally{
        await Client.close();
    }


}
// PoblarPedidos(2000);

//CRUD
//FIND
async function FindOnePedido(idPedido){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").findOne({idPedido: idPedido});
        if(result){
            console.log(`Se encontro un pedido con el siguiente id: ${idPedido}`);
            console.log(result);
        }else{
            console.log(`No se encontro un pedido con el siguiente id: ${idPedido}`);
        }
    }catch(e){
        console.log(e);
    }finally{
        await Client.close();
    }
}

// FindOnePedido(2006);

async function FindPedido(){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").find({}).project({idPedido: true, estadoPedido: true, tipoPagoPedido: true}).sort({tipoPagoPedido: 1}).limit(10).toArray();
        if(result.length > 0){
            console.log(`Se encontraron registros`);
            console.log(result);
        }else{
            console.log(`No se encontraron registros`);
        }
    }catch(e){
        console.log(e);
    }finally{
        await Client.close();
    }
}

// FindPedido();

//CREATE
async function insertOnePedido(nuevoPedido){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").insertOne(nuevoPedido);
        if(result){
            console.log(`Se creo un nuevo pedido con el siguiente id: ${result.insertedId}`);
            console.log(nuevoPedido);
        }else{
            console.log(`No se creo ningun pedido`);
        }
       
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// insertOnePedido({
//     idPedido: 2006,
//     dni: 1013457130,
//     idProducto: 2,
//     estadoPedido: "Confirmar",
//     fechaPedido: new Date("2023-06-30T05:27:08.754+00:00"),
//     tipoPagoPedido: "Efectivo",
//     totalPedido: 50000,
// });


async function insertManyPedido(nuevoPedido){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").insertMany(nuevoPedido);
        if(result){
            console.log(`Se creo ${result.insertedCount} nuevos pedidos`);
            console.log(nuevoPedido);
        }else{
            console.log("No se insertaron los datos");
        }   
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
    }
  ];
  
// insertManyPedido(nuevoPedido);

//UPDATE
async function updateOnePedido(Pedido, atributoCambio){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").updateOne
        ({idPedido: Pedido},{$set: atributoCambio});
        if(result){
            console.log(`${result.matchedCount} documento cumple con el criterio de busqueda`);
            console.log(`${result.modifiedCount} documento fue actualizado`);
            console.log(result);
        }else{
            console.log("No se actualizo ningun pedido");
        }
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// updateOnePedido(2006, {idProducto: 5, estadoPedido: "enviado", tipoPagoPedido: "Transferencia"});

async function updateManyPedido(estado, atributoCambio){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").updateMany
        ({estadoPedido: estado},{$set:{estadoPedido: atributoCambio}});
        if(result){
            console.log(`${result.matchedCount} documentos cumplen con el criterio de busqueda`);
            console.log(`${result.modifiedCount} documentos fueron actualizados`);
            console.log(result);
        }else{
            console.log("No se actualizo ningun pedido");
        }    
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// updateManyPedido({}, {});

//DELETE
async function deleteOnePedido(eliminarPedido){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").deleteOne(eliminarPedido);
        if(result){
            console.log(`${result.deletedCount} pedido eliminado`);
        }else{
            console.log("No se elimino ningun pedido");
        }
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// deleteOnePedido({idPedido: 2006});

async function deleteManyPedido(eliminarPedidos){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Pedidos").deleteMany(eliminarPedidos);
        if(result){
            console.log(`${result.deletedCount} pedidos eliminados`);
        }else{
            console.log("No se elimino ningun pedido");
        }    
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// deleteManyPedido({estadoPedido:});

async function dropCollectionPedido(eliminarColeccion){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection(eliminarColeccion).drop();
        if(result){
            console.log(`La colección ${eliminarColeccion} ha sido eliminada`);
        }else{
            console.log("No se elimino la colección");
        }    
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
        if(result){
            console.log(`La base de datos ${eliminarBasedeDatos} ha sido eliminada`);
        }else{
            console.log("No se elimino la base de datos");
        }  
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
            },{
                $unwind: "$datosComprador"
            }
        ]).toArray();
        console.log("Consulta exitosa");
        console.log(result);
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
                $unwind: "$productos"
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
        console.log(result);
    } catch (e) {
      console.log(e);
    } finally {
      await Client.close();
    }
}
  
// aggregate2Pedidos();