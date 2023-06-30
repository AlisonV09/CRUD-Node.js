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
        const Result= await Client.db('SoftDCano').collection('Ventas').insertMany(Datos);
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

FindOneVenta(2023);

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

// FindVenta({});

//CREATE
async function insertOneVenta(nuevaVenta){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").insertOne(nuevaVenta);
        if(result){
            console.log(`Se creo una nueva venta con el siguiente id: ${result.insertedId}`);
            console.log(nuevaVenta);
        }else{
            console.log("No se creo la venta");
        } 
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}
// insertOneVenta({
//     idVenta: 2023,
//     idUsuario: 1013457130,
//     idPedido: 4,
//     dni: 1013457138,
//     fechaVenta: new Date("2023-06-30T05:27:15.754+00:00"),
//     estadoVenta: "Cancelada",
//     tipoPagoVenta: "Transferencia",
//     totalVenta: 60000,
//     comprobante: "https://loremflickr.com/640/480?lock=2212244194590720",
// });


async function insertManyVenta(nuevaVenta){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").insertMany(nuevaVenta);
        if(result){
            console.log(`Se creo ${result.insertedCount} nuevas ventas`);
            console.log(nuevaVenta);
        }else{
            console.log("No se creo ninguna venta");
        }
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
    }
  ];
  
// insertManyVenta(nuevaVenta);

//UPDATE
async function updateOneVenta(IdVenta, atributoCambio){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").updateOne
        ({idVenta: IdVenta},{$set:{totalVenta: atributoCambio}});
        if(result){
            console.log(`${result.matchedCount} documento cumple con el criterio de busqueda`);
            console.log(`${result.modifiedCount} documento fue actualizado`);
            console.log(result);
        }else{
            console.log("No se actualizo la venta");
        }
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// updateOneVenta(2023, 100000);

async function updateManyVenta(estaVenta, atributoCambio){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").updateMany
        ({estadoVenta: estaVenta},{$set:{estadoVenta: atributoCambio}});
        if(result){
            console.log(`${result.matchedCount} documentos cumplen con el criterio de busqueda`);
            console.log(`${result.modifiedCount} documentos fueron actualizados`);
            console.log(result);
        }else{
            console.log("No se actualizo ninguna venta");
        }
        
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// updateManyVenta({}, {});

//DELETE
async function deleteOneVenta(eliminarVenta){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").deleteOne(eliminarVenta);
        if(result){
            console.log(`${result.deletedCount} venta eliminada`);
        }else{
            console.log("No se elimino la venta");
        } 
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// deleteOneVenta({idVenta: 2023});

async function deleteManyVenta(eliminarVentas){
    const Client = new MongoClient(URI);

    try{
        await Client.connect();
        const result = await Client.db("SoftDCano").collection("Ventas").deleteMany(eliminarVentas);
        if(result){
            console.log(`${result.deletedCount} ventas eliminadas`);
        }else{
            console.log("No se elimino ninguna venta");
        }   
    }catch(e){ 
        console.log(e);
    }finally{
        Client.close();
    }
}

// deleteManyVenta({estadoVenta:});

async function dropCollectionVenta(eliminarColeccion){
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

// dropCollectionVenta("Ventas");

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
            },{
                $unwind: "$pedidos" 
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
                $unwind: "$usuario" 
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
        console.log(result);
    } catch (e) {
      console.log(e);
    } finally {
      await Client.close();
    }
}