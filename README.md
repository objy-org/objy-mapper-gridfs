# OBJY storage mapper for gridfs


## Installing

```
npm install objy-mapper-gridfs
```


## Example

Let's create an Object Family that uses the mapper:

```
const GridFSMapper = require('objy-mapper-gridfs');

// Define an object family
OBJY.define({
   name : "Object",
   pluralName: "Objects",
   storage: new GridFSMapper().connect('mongodb://localhost'),
})

// Use the object family's constructor
OBJY.Object({name: "Hello World"}).add(function(data)
{
   console.log(data);
})
```

## License

This project itself is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details. 
