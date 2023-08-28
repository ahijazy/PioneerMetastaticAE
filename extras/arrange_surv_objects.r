 
 
arrange_tte_objects=function(object)
{ 
IRsettings = read.csv("./inst/settings/IRsettings.csv")

# Predetermined list of target and outcome IDs
target_ids <- unique(IRsettings$target_id)
outcome_ids <- unique(IRsettings$outcome_id)

# Create an empty matrix to store survfit objects
matrix_list <- array(list(), dim = c(length(target_ids), length(outcome_ids)),
                     dimnames = list(target_ids, outcome_ids))

# Fill the matrix_list with survfit objects
for (i in seq_along(object)) {
  
    target <- object[[i]]$target
    outcome <- object[[i]]$outcome
	survfit_obj <- object[[i]] 
	row_t=which(target==rownames(matrix_list))
	col_o=which(outcome==colnames(matrix_list))
      matrix_list[[row_t, col_o]] <- survfit_obj
}
 
return(matrix_list)
}
 