# delete the v from the version tag cause python build seems to strip it as well
version = $(shell git tag | tail -1 | tr -d v)

all:
	python3 -m build --no-isolation

install:
	make
	pip install "./dist/preplib-${version}-py3-none-any.whl" --no-deps --force-reinstall

doc:
	pdoc --html src/preplib --force

publish:
	gh release create "v${version}" "./dist/preplib-${version}-py3-none-any.whl"

publish-update: # if an asset was already uploaded, delete it before uploading again
	gh release delete-asset "v${version}" "preplib-${version}-py3-none-any.whl" -y
	gh release upload "v${version}" "./dist/preplib-${version}-py3-none-any.whl"