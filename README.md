# jackrabbit-oak-orientdb

TODO: description ![Maven Central](https://img.shields.io/maven-central/v/com.github.monkeysintown/jackrabbit-oak-orientdb.svg)



If you just want to compile the project without running the tests:

```
mvn -DskipTests clean install
```

If you want to run the tests (Derby and H2 in server mode):

```
mvn clean install
```


You can find the latest releases here:

[ ![Download](https://api.bintray.com/packages/cheetah/monkeysintown/jackrabbit-oak-orientdb/images/download.svg) ](https://bintray.com/cheetah/monkeysintown/jackrabbit-oak-orientdb/_latestVersion)

... or setup your Maven dependencies:

```xml
<dependency>
    <groupId>com.github.monkeysintown</groupId>
    <artifactId>jackrabbit-oak-orientdb</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

... and configure Bintray's JCenter repository in your pom.xml:
 
```xml
...
<repositories>
    <repository>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
        <id>central</id>
        <name>bintray</name>
        <url>http://jcenter.bintray.com</url>
    </repository>
</repositories>
...
```

Get automatic notifications about new releases here:

[ ![Get automatic notifications about new "jackrabbit-oak-orientdb" versions](https://www.bintray.com/docs/images/bintray_badge_color.png) ](https://bintray.com/cheetah/monkeysintown/jackrabbit-oak-orientdb/view?source=watch)
