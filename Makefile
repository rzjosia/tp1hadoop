DEST=/usr/lib
SOURCE=./.config/commons-csv-1.6

all: copy

copy:
	sudo cp -ru ${SOURCE} ${DEST}
