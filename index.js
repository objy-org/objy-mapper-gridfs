var mongoose = require('mongoose');
var Schema = mongoose.Schema;
var Admin = mongoose.mongo.Admin;

const generalObjectModel = {
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
    authorisations: {},
};


const ObjSchema = new Schema(generalObjectModel, { strict: false });

Mapper = function (OBJY, options) {
    return Object.assign(new OBJY.StorageTemplate(OBJY, options), {
        database: {},
        databases: {},
        currentConnectionString: null,
        index: {},
        globalPaging: 20,

        connect: function (connectionString, success, error, options) {
            this.currentConnectionString = connectionString;
            this.database = mongoose.createConnection(connectionString, options);

            this.database.on('error', function (err) {
                error(err);
            });

            this.database.once('open', function () {
                success();
                this.currentConnectionString = connectionString;
            });

            return this;
        },

        getDBConnection: async function (dbName) {
            if (!this.databases[dbName]) {
                if (dbName.match(/[\/\\."$\*<>:\|\?]/)) {
                    return null;
                }

                this.databases[dbName] = await mongoose.createConnection(this.currentConnectionString, { dbName: dbName });
                return this.databases[dbName];
            }

            if (this.databases[dbName]) {
                return this.databases[dbName];
            }
        },

        getConnection: function () {
            return { database: this.database, databases: this.databases, currentConnectionString: this.currentConnectionString };
        },

        useConnection: function (connection, success, error) {
            this.database = connection.database;
            this.databases = connection.databases;
            this.currentConnectionString = connection.currentConnectionString;

            this.database.on('error', function (err) {
                error(err);
            });

            this.database.once('open', function () {
                success();
            });

            return this;
        },

        getDBByMultitenancy: async function (client) {
            if (this.staticDatabase) return await this.getDBConnection(client);

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED) {
                return await this.getDBConnection('spoo');
            } else if (this.multitenancy == this.CONSTANTS.MULTITENANCY.ISOLATED) {
                return await this.getDBConnection(client);
            }
        },

        createClient: async function (client, success, error) {
            var db = await this.getDBByMultitenancy(client);

            var ClientInfo = db.model('clientinfos', ClientSchema);

            ClientInfo.find({ name: client }).exec(function (err, data) {
                if (err) {
                    error(err);
                    return;
                }
                if (data.length >= 1) {
                    error('client name already taken');
                } else {
                    new ClientInfo({ name: client }).save(function (err, data) {
                        if (err) {
                            error(err);
                            return;
                        }

                        success(data);
                    });
                }
            });
        },

        listClients: async function (success, error) {
            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.ISOLATED) {
                new Admin(this.database.db).listDatabases(function (err, result) {
                    if (err) error(err);
                    success(
                        result.databases.map(function (item) {
                            return item.name;
                        }),
                    );
                });
            } else {
                var db = await this.getDBByMultitenancy('spoo');

                var ClientInfo = db.model('clientinfos', ClientSchema);

                ClientInfo.find({}).exec(function (err, data) {
                    if (err) {
                        error(err);
                        return;
                    }

                    success(
                        data.map(function (item) {
                            return item.name;
                        }),
                    );
                });
            }
        },

        getById: async function (id, success, error, app, client) {
            const db = await this.getDBByMultitenancy(client);
            const bucket = new mongoose.mongo.GridFSBucket(db);

            let data = null;

            var constrains = null;

            try {
                constrains = { _id: id };
            } catch (e) {
                OBJY.Logger.error(e);
                error('Error getting file invalid id');
                return;
            }

            if (app) constrains['applications'] = { $in: [app] };

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) constrains['tenantId'] = client;

            Obj = db.model(this.objectFamily, ObjSchema);

            try {
                //var _data = Attachment.read({ _id: mongoose.Types.ObjectId(id) });
                let downloadStream = null;

                try {
                    downloadStream = bucket.openDownloadStream(new mongoose.Types.ObjectId(id));
                } catch (err) {
                    error(err);
                    return;
                }

                try {
                    data = await Obj.findOne(constrains);
                } catch (err) {
                    error(err);
                    return;
                }

                if (!data) return error({ error: 'file not found' });

                if (!data.properties) data.properties = {};

                data.properties.data = downloadStream;

                success(data);
                return;

                //success({ _id: id, name: id, role: this.objectFamily, properties: { data: data } });
            } catch (e) {
                OBJY.Logger.error(e);
                error('Error getting file');
            }
        },

        getByCriteria: async function (criteria, success, error, app, client, flags) {
            const db = await this.getDBByMultitenancy(client);
            let data = null;

            var Obj = db.model(this.objectFamily, ObjSchema);

            if (flags.$page == 1) flags.$page = 0;
            else flags.$page -= 1;

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            if (criteria.$query) {
                criteria = JSON.parse(JSON.stringify(criteria.$query));
                delete criteria.$query;
            }

            var arr = [{ $match: criteria }, { $limit: 20 }];
            if (flags.$page) arr.push({ $skip: this.globalPaging * (flags.$page || 0) });

            var s = {};

            if (flags.$sort) {
                if (flags.$sort.charAt(0) == '-') {
                    s[flags.$sort.slice(1)] = -1;
                } else {
                    s[flags.$sort] = 1;
                }

                arr.push({ $sort: s });
            }

            if (app) criteria['applications'] = { $in: [app] };

            var finalQuery = Obj.find(criteria);

            if (flags.$limit) finalQuery.limit(flags.$limit).sort(s || { _id: 1 });
            else
                finalQuery
                    .limit(this.globalPaging)
                    .skip(this.globalPaging * (flags.$page || 0))
                    .sort(s || { _id: 1 });

            if (criteria.$aggregate) {
                finalQuery = Obj.aggregate(criteria.$aggregate);
            }

            try {
                data = await finalQuery.lean().exec();
            } catch (err) {
                error(err);
                return;
            }

            success(data);
            return;
        },

        count: async function (criteria, success, error, app, client, flags) {
            var db = await this.getDBByMultitenancy(client);
            let data = null;

            var Obj = db.model(this.objectFamily, ObjSchema);

            if (criteria.$query) {
                criteria = JSON.parse(JSON.stringify(criteria.$query));
                delete criteria.$query;
            }

            if (app) criteria['applications'] = { $in: [app] };

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            try {
                data = await Obj.count(criteria).exec();
            } catch (err) {
                error(err);
                return;
            }

            success({ result: data });
            return;
        },

        update: function (spooElement, success, error, app, client) {
            error('Update not possible');
        },

        add: async function (spooElement, success, error, app, client) {
            const db = await this.getDBByMultitenancy(client);
            const bucket = new mongoose.mongo.GridFSBucket(db);

            let data = null;

            //spooElement = spooElement.properties.data;

            var Obj = db.model(this.objectFamily, ObjSchema);

            try {
                var fileId = new mongoose.Types.ObjectId();

                var fileData = spooElement.properties.data;

                const uploadStream = bucket.openUploadStream(spooElement.name, {
                    metadata: {},
                });

                try {
                    await new Promise((resolve, reject) => {
                        fileData
                            .pipe(uploadStream)
                            .on('error', (err) => {
                                console.log(err);
                                reject(err);
                            })
                            .on('finish', (data) => {
                                console.log('finish', uploadStream.id);
                                resolve(data);
                            });
                    });
                } catch (err) {
                    console.log(err);
                    error('Error adding file');
                    return;
                }

                var _spooElement = Object.assign({}, spooElement);

                if (uploadStream.id) _spooElement._id = uploadStream.id.toString();
                else _spooElement._id = fileId.toString();

                if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED) _spooElement.tenantId = client;

                delete _spooElement.properties.data;
                _spooElement.mimetype = _spooElement.properties.mimetype + '';
                delete _spooElement.properties.mimetype;

                try {
                    data = await Obj(_spooElement).save();
                } catch (err) {
                    error(err);
                    return;
                }

                success(data);

                /*
                Attachment.write({ _id: fileId, filename: spooElement.name }, fileData, (err, file) => {
                    if (err) {
                        console.log(err);
                        error('Error adding file');
                        return;
                    }

                    var _spooElement = Object.assign({}, spooElement);

                    if (file) _spooElement._id = file._id.toString();
                    else _spooElement._id = fileId.toString();

                    if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED) _spooElement.tenantId = client;

                    delete _spooElement.properties.data;
                    _spooElement.mimetype = _spooElement.properties.mimetype + '';
                    delete _spooElement.properties.mimetype;
                    new Obj(_spooElement).save(function (err, data) {
                        if (err) {
                            error(err);
                            return;
                        }
                        success(data);
                    });
               

                    //success({ _id: file._id, name: fileId })
                });
                */
            } catch (e) {
                OBJY.Logger.error(e);
                error('Error adding file');
            }
        },
        remove: async function (spooElement, success, error, app, client) {
            const db = await this.getDBByMultitenancy(client);
            const bucket = new mongoose.mongo.GridFSBucket(db);

            let data = null;

            var Obj = db.model(this.objectFamily, ObjSchema);

            var criteria = { _id: spooElement._id };

            if (app) criteria['applications'] = { $in: [app] };

            if (this.multitenancy == this.CONSTANTS.MULTITENANCY.SHARED && client) criteria['tenantId'] = client;

            try {
                data = await bucket.delete(new mongoose.Types.ObjectId(spooElement._id));
            } catch (err) {
                OBJY.Logger.error(err);
                error('Error removing file');
                return;
            }

            try {
                data = await Obj.deleteOne(criteria);
            } catch (err) {
                console.log(err);

                return error('Error removing file 2', err);
            }

            if (data.n == 0) error('object not found');
            else {
                success({ _id: spooElement._id });
            }
        },
    });
};

module.exports = Mapper;
