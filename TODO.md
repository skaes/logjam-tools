# TODO

* insert load balancers (or simple proxies) between parsers and
  request writers and also between controller and stats updaters.
  This way we can move the knowledge how many parsers/updaters/writers
  exist to a single place: the controller.
* should send sent_at field with logjam request data (UTC) to detect clock
  drift more reliably
