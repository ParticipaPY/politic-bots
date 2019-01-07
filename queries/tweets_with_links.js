db.getCollection("tweets").aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: {
			            'relevante': {'$eq': 1},
			            'tweet_obj.entities.media': {'$exists': 0},
			            '$and': [
			                {'tweet_obj.entities.urls': {'$ne': []}},  
			                {'tweet_obj.truncated': False}, 
			                {'$or': [
			                    {'tweet_obj.is_quote_status': False},
			                    {'$and': [{'tweet_obj.is_quote_status': True}, {'tweet_obj.entities.urls': {'$size': 2}}]}
			                ]}
			                ]
			        }
		},

		// Stage 2
		{
			$match: {'$or': [{'tweet_obj.retweeted_status': {'$exists': 0}},
			                              {'$and': [{'tweet_obj.retweeted_status': {'$exists': 1}},
			                                        {'tweet_obj.is_quote_status': True}]}]}
		},

		// Stage 3
		{
			$match: {
			  $or: [
			    { "flag.partido_politico.anr": {$gt: 0} },
			    { "flag.partido_politico.plra": {$gt: 0} },
			    { "flag.partido_politico.pdp": {$gt: 0} }
			  ]
			}
		},

		// Stage 4
		{
			$project: {
			    anr: {$cond: {if: {$gt: ["$flag.partido_politico.anr", 0]}, then: 1, else: 0}},
			    plra: {$cond: {if: {$gt: ["$flag.partido_politico.plra", 0]}, then: 1, else: 0}},
			    pdp: {$cond: {if: {$gt: ["$flag.partido_politico.pdp", 0]}, then: 1, else: 0}},
			    all: "all"
			}
		},

		// Stage 5
		{
			$group: {
				_id: "$all", 
				"anr": {$sum: "$anr" },
				"plra": {$sum: "$plra" },
				"pdp": {$sum: "$pdp" }
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
