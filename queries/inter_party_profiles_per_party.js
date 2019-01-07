db.getCollection("users").aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: { "is_potential_troll": 1}
		},

		// Stage 2
		{
			$group: {
				_id: {
					"party": "$party",
					"most_interacted_party": "$most_interacted_party"
					
				},
				count: {$sum: 1}
			}
		},

		// Stage 3
		{
			$project: {
			    "party-tuple": {$concat : ["$_id.party", "-", "$_id.most_interacted_party"] },
			    count: "$count"
			}
		},

		// Stage 4
		{
			$sort: {
			  "party-tuple": -1
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
