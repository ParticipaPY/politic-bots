db.getCollection("users").aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: { $or: [ { $and: [ { "exists": NumberInt(0) }, { "bot_analysis.pbb": { $gt: 1.465753425 } } ] }, { $and: [ { "exists": NumberInt(1) }, { "bot_analysis.pbb": { $gt: 1.716666667 } } ] } ] }
		},

		// Stage 2
		{
			$project: {
				ors_total: "$original_tweets",
				rts_total: "$rts",
				rps_total: "$rps",
				qts_total: "$qts", 
				_id: "all" 
			}
		},

		// Stage 3
		{
			$group: {
			  _id: "$_id",
			  ors_total: { $sum: "$ors_total"},
			  rts_total: { $sum:  "$rts_total"},
			  rps_total: { $sum:  "$rps_total"},
			  qts_total: { $sum:  "$qts_total"}
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
