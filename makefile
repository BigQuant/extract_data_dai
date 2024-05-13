dev:
	bq module install --dev

undev:
	bq module uninstall --dev

publish:
	bq module publish
	# git push --tags
