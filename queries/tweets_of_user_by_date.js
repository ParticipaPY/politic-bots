db.getCollection("tweets").aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: { "tweet_obj.user.screen_name": "EfrainAlegre" }
		},

		// Stage 2
		{
			$group: {
				_id: "$tweet_py_date",
				count: {$sum: 1}
			}
		},

		// Stage 3
		{
			$match: {
			
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
