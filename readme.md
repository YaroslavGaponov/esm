migrate
===================
prototype for migrate elasticsearch indices

# Info
This library streamify the process export/import data from/to Elasticsearch

### example save data to file from elasticsearch
```
elasticsearch
    .pipe(new MyModifier1())
    .pipe(new MyModifier2())
    ...
    .pipe(new MyModifierN())
    .pipe(pack)
    .pipe(gzip)
    .pipe(file)
;
```

### example load data from file to elasticsearch
```
file
    .pipe(gunzip)
    .pipe(unpack)
    .pipe(new MyModifier1())
    .pipe(new MyModifier2())
    ...
    .pipe(new MyModifierN())
    .pipe(elasticsearch)
;

```

# Demo
Create proto file
```
npm run proto
```

Save data from Elasticsearch to file
```
npm run save
```

Load data to Elasticsearch from file
```
npm run load
```

# Simple modifier
```
class MyModifier extends SimpleModifier {
    constructor(options) {
        super(options);        
    }
    _transform(asset, encoding, callback) {
        const newAsset = SomeModifier(asset);
        this.push(newAsset);        
        callback();
    }
}

```

# Simple cloner
```
class MyModifier extends SimpleModifier {
    constructor(options) {
        super(options);        
    }
    _transform(asset, encoding, callback) {
        const index = asset.index;
        const type = asset.type;
        for(let i=0; i<10; i++) {
            asset.index = index + '_' + i;
            asset.type  = type  + '_' + i;
            this.push(asset);
        }
        callback();
    }
}

```

