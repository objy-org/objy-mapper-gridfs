var mongoose = require('mongoose');
var Grid = require('gridfs-stream');
var Schema = mongoose.Schema;
var Admin = mongoose.mongo.Admin;
var shortid = require('shortid');

var createModel;

var Attachment;

var generalObjectModel = {
    type: { type: String, index: true },
    applications: { type: [String], index: true },
    created: { type: String, index: true },
    lastModified: { type: String, index: true },
    role: String,
    inherits: [],
    name: String,
    mimetype: String,
    onDelete: {},
    onCreate: {},
    onChange: {},
    permissions: {},
    properties: {},
    privileges: {},
    aggregatedEvents: [],
    tenantId: String,
    password: String,
    username: String,
    email: String,
    _clients: [],
    authorisations: {}
};

var ObjSchema = new Schema(generalObjectModel, { strict: false });

const mongo = mongoose.mongo;

Mapper = function(OBJY, options) {
    return Object.assign(new OBJY.StorageTemplate(OBJY, options), {

        database: {},
        index: {},
        globalPaging: 20,

        connect: function(connectionString, success, error, options) {
            this.database = mongoose.createConnection(connectionString, options);

            this.database.on('error', function(err) {
                error(err)
            });

            this.database.once('open', function() {
                success();
                createModel = require('mongoose-gridfs').createModel;
            });

            return this;
        },

        getConnection: function() {
            return this.database;
        },

        useConnection: function(connection, success, error) {
            this.database = connection;

            this.database.on('error', function(err) {
                error(err)
            });

            this.database.once('open', function() {
                success();
            });

            return this;
        },

        getDBByMultitenancy: function(client) {

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED) {
                return this.database.useDb('spoo')
            } else if (this.multitenancy == this.CONSTANTS.MULTITENANCY.ISOLATED) {
                return this.database.useDb(client)
            }
        },

        createClient: function(client, success, error) {

            var db = this.getDBByMultitenancy(client);

            var ClientInfo = db.model('clientinfos', ClientSchema);

            ClientInfo.find({ name: client }).exec(function(err, data) {
                if (err) {
                    error(err);
                    return;
                }
                if (data.length >= 1) {
                    error("client name already taken")
                } else {

                    new ClientInfo({ name: client }).save(function(err, data) {
                        if (err) {

                            error(err);
                            return;
                        }

                        success(data);

                    })
                }

            });
        },

        listClients: function(success, error) {

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.ISOLATED) {

                new Admin(this.database.db).listDatabases(function(err, result) {
                    if (err) error(err)
                    success(result.databases.map(function(item) {
                        return item.name
                    }));
                });

            } else {
                var db = this.getDBByMultitenancy('spoo');

                var ClientInfo = db.model('clientinfos', ClientSchema);

                ClientInfo.find({}).exec(function(err, data) {

                    if (err) {

                        error(err);
                        return;
                    }

                    success(data.map(function(item) {
                        return item.name
                    }))

                });
            }
        },

        getById: function(id, success, error, app, client) {

            var db = this.getDBByMultitenancy(client);

            var Attachment = createModel({
                modelName: 'File',
                connection: db
            });
            var constrains = null;

            try {
                constrains = { _id: mongoose.Types.ObjectId(id) }; 
            } catch(e){
                OBJY.Logger.error(e);
                error('Error getting file invalid id')
                return
            }

            if (app) constrains['applications'] = { $in: [app] }

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) constrains['tenantId'] = client;

            Obj = db.model(this.objectFamily, ObjSchema);

            try {
                var _data = Attachment.read({ _id: mongoose.Types.ObjectId(id) })

                Obj.findOne(constrains, function(err, data) {
                    if (err) {
                        error(err);
                        return;
                    }

                    if(!data) return error({error: 'file not found'});

                    if (!data.properties) data.properties = {};

                    data.properties.data = _data;

                    success(data);
                    return;
                });

                //success({ _id: id, name: id, role: this.objectFamily, properties: { data: data } });
            } catch (e) {
                OBJY.Logger.error(e);
                error('Error getting file')
            }
        },

        getByCriteria: function(criteria, success, error, app, client, flags) {

            var db = this.getDBByMultitenancy(client);

            var Obj = db.model(this.objectFamily, ObjSchema);


            if (flags.$page == 1) flags.$page = 0;
            else flags.$page -= 1;

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            if (criteria.$query) {
                criteria = JSON.parse(JSON.stringify(criteria.$query));
                delete criteria.$query;
            }

            var arr = [{ $match: criteria }, { $limit: 20 }];
            if (flags.$page) arr.push({ $skip: this.globalPaging * (flags.$page || 0) })

            var s = {};

            if (flags.$sort) {

                if (flags.$sort.charAt(0) == '-') {
                    s[flags.$sort.slice(1)] = -1;
                } else {
                    s[flags.$sort] = 1;
                }

                arr.push({ $sort: s })
            }

            if (app) criteria['applications'] = { $in: [app] }

            var finalQuery = Obj.find(criteria)

            if (flags.$limit) finalQuery.limit(flags.$limit).sort(s || { '_id': 1 });
            else finalQuery.limit(this.globalPaging).skip(this.globalPaging * (flags.$page || 0)).sort(s || { '_id': 1 });

            if (criteria.$aggregate) {
                finalQuery = Obj.aggregate(criteria.$aggregate);
            }

            finalQuery.lean().exec(function(err, data) {
                if (err) {
                    error(err);
                    return;
                }

                success(data);
                return;
            });



        },

        count: function(criteria, success, error, app, client, flags) {

            var db = this.getDBByMultitenancy(client);

            var Obj = db.model(this.objectFamily, ObjSchema);

            if (criteria.$query) {
                criteria = JSON.parse(JSON.stringify(criteria.$query));
                delete criteria.$query;
            }

            if (app) criteria['applications'] = { $in: [app] }

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            Obj.count(criteria).exec(function(err, data) {
                if (err) {
                    error(err);
                    return;
                }

                success({ 'result': data });
                return;
            });
        },

        update: function(spooElement, success, error, app, client) {

            error('Update not possible')
        },

        add: function(spooElement, success, error, app, client) {

            var db = this.getDBByMultitenancy(client);

            var Attachment = createModel({
                modelName: 'File',
                connection: db
            });

            //spooElement = spooElement.properties.data;

            var Obj = db.model(this.objectFamily, ObjSchema);

            try {

                var fileId = new mongoose.Types.ObjectId();

                var fileData = spooElement.properties.data;

                Attachment.write({ _id: fileId, filename: spooElement.name }, fileData, (err, file) => {
                    

                    if (err) {
                        console.log(err); 
                        error('Error adding file');
                        return;
                    }


                    var _spooElement = Object.assign({}, spooElement)

                    if(file) _spooElement._id = file._id.toString();
                    else _spooElement._id = fileId.toString();

                    if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED) _spooElement.tenantId = client;

                    delete _spooElement.properties.data;
                    _spooElement.mimetype = _spooElement.properties.mimetype + '';
                    delete _spooElement.properties.mimetype;
                    new Obj(_spooElement).save(function(err, data) {
                        if (err) {
                            error(err);
                            return;
                        }
                        success(data);
                    })

                    //success({ _id: file._id, name: fileId })
                });

            } catch (e) {
                OBJY.Logger.error(e);
                error('Error adding file')
            }
        },
        remove: function(spooElement, success, error, app, client) {

            var db = this.getDBByMultitenancy(client);

            var Attachment = createModel({
                modelName: 'File',
                connection: db
            });


            var Obj = db.model(this.objectFamily, ObjSchema);

            var criteria = { _id: spooElement._id };

            if (app) criteria['applications'] = { $in: [app] }

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;




            Attachment.unlink({ _id: spooElement._id }, (err, file) => {
                if (err) {
                    OBJY.Logger.error(err);
                    error('Error removing file');
                    return
                }

                Obj.deleteOne(criteria, function(err, data) {
                    if (err) {
                        error(err);
                        return;
                    }
                    if (data.n == 0) error("object not found");
                    else {
                        success({ _id: spooElement._id });
                    }

                })

                //success({ _id: spooElement._id })
            });
        }
    })
}



module.exports = Mapper;