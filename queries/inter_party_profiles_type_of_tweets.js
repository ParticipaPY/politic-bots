db.getCollection("users").aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$match: { "is_potential_troll": 1}
		},

		// Stage 2
		{
			$project: {
			    ors_total: { $sum : "$original_tweets"},
			    rts_total: { $sum : "$rts"},
			    rps_total: { $sum : "$rps"},
			    qts_total: { $sum : "$qts"}, 
			    _id: "$party" 
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
