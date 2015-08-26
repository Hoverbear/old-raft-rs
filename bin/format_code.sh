#!/bin/sh
for i in `find src examples -type f -name '*.rs'`
	do rustfmt --write-mode=overwrite $i
done
