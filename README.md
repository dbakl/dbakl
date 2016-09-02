# dbakl
All in one development kit


## Usage

### Connecting the Database
Setting up the Connection

```
$con = new MySqlConnection(new MySqlDriver());
$con->connect();
```


### Persistence Manager

```
$pm->load($e = new SomeEntity(), ["id"=>"SomeValue"]);
$pm->store($e);
$pm->delete($e);
```


### FluentSql

```
$pm->fsql()->select()->from(Entity::class, "a")->leftJoin(Entity2::class, "b")->then()->each(
    function (Entity $e, Entity2 $e2) {
    }
);
```

or - more abstract - let the framework build the object

```
$pm->fsql()->select()->from(Entity::class, "a")->leftJoin(Entity2::class, "b", [Entity::class, "bs", true])->then()->each(
    function 
);
```

will fill the Entity Objects property "bs" with Entities.


### Upgrade a struct

```
$pm->store($entity);
```


