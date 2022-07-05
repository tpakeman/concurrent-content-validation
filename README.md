# concurrent-content-validation
### Basics
* Initial implementation of an idea to concurrently run multiple Looker content validator runs, each one scanning 1/nth of the available content
* The full idea is below (with an indication of if it is implemented):
  * Parse all of the Looker folders ✅
    * Preserve parent-child relationships ✅
    * Identify enclosed dashboards and Looks ✅ 
    * Count the total number of enclosed queries ✅
  * Split the folders into `n` slices of ~equal query volume ✅ (needs checking)
    * Capture their `content_metadata_ids`  ✅
        * Ensure each exists in a path containing all parent folder IDs for inheritance ✅ (needs checking)
  * Create `n` users with the `develop` permission and no folder access ❌
    * Give each user access to one of the `n` slices of content ❌
    * Run the validator for each user ❌

### Bugs + missing features
* There are still a number of SDK errors being raised by the content validator
* This does not yet handle user creation or deletion
* This only works with a closed system enabled
* This does not scan LookML dashboards (but neither does the content validator)
* This only imperfectly divides queries into 1/n fractions (and needs to be tested)