# capnp_sample

~~~
mkdir build
cd build
cmake ..
make

./calculator-server unix:/tmp/test &
./calculator-client unix:/tmp/test
echo -en "test\n" | socat unix-sendto:/tmp/sample-server.sock stdin
~~~

- Reference Link
https://github.com/capnproto/capnproto/issues/1000