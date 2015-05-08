# TODO

* insert load balancers (or simple proxies) between parsers and
  request writers and also between controller and stats updaters.
  This way we can move the knowledge how many parsers/updaters/writers
  exist to a single place: the controller.

* make number of parsers configurable (command line/config file)
