<?php
function custom_admin_tag_menu()
{
    // Add the main menu page
    add_menu_page(
        'Tag Library',
        'Tag Library',
        'manage_options',
        'tag_menu',
        'tag_manage_page_callback',
        'dashicons-tag',
        7
    );

    add_submenu_page(
        'tag_menu',
        'Add New Tag',
        'Add New Tag',
        'manage_options',
        'add_new_tag',
        'add_new_tag_page_callback'
    );
}
add_action('admin_menu', 'custom_admin_tag_menu');


// Tag Manage
function tag_manage_page_callback()
{
    global $wpdb;
    $tag_groups = $wpdb->get_results("
        SELECT t.term_id, t.name as group_name, tm.meta_value as sort, tm2.meta_value as state
        FROM {$wpdb->terms} t
        INNER JOIN {$wpdb->term_taxonomy} tt ON tt.term_id = t.term_id AND tt.taxonomy = 'all-tags'
        LEFT JOIN {$wpdb->termmeta} tm ON tm.term_id = t.term_id AND tm.meta_key = 'sort'
        LEFT JOIN {$wpdb->termmeta} tm2 ON tm2.term_id = t.term_id AND tm2.meta_key = 'state'
        WHERE tt.description = 'parent-tags'
    ");
?>
    <div class="wrap">
        <h1>Tag Management</h1>
        <!-- Add New Tag Button -->
        <a href="<?php echo admin_url('admin.php?page=add_new_tag'); ?>" class="button button-primary" style=" margin-left: 1200px; margin-bottom: 20px;">+ Add New Tag</a>
        <table class="widefat fixed" style="width: 100%; margin-bottom: 20px;">
            <thead>
                <tr>
                    <th>Tag Group</th>
                    <th>Tag Name(s)</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                <?php if (!empty($tag_groups)) {
                    foreach ($tag_groups as $tag_group) {
                        // Get associated tag names
                        $tag_names = $wpdb->get_results("
                            SELECT t.name
                            FROM {$wpdb->terms} t
                            INNER JOIN {$wpdb->term_taxonomy} tt ON tt.term_id = t.term_id
                            WHERE tt.parent = {$tag_group->term_id} AND tt.description = 'sub-tags'
                        ");


                        $tag_names_list = !empty($tag_names) ? implode(', ', wp_list_pluck($tag_names, 'name')) : 'No tags';
                ?>
                        <!-- foreach ($tag_manage as $tag_id => $tag_data) { ?> -->
                        <tr>
                            <td><?php echo esc_html($tag_group->group_name); ?></td>
                            <td><?php echo esc_html($tag_names_list); ?></td>
                            <td>
                                <a href="<?php echo admin_url('admin.php?page=view-edit-tag&tag_id=' . $tag_group->term_id . '&action=edit'); ?>" class="button">Edit</a>
                                <a href="<?php echo admin_url('admin.php?page=view-edit-tag&tag_id=' . $tag_group->term_id); ?>" class="button">View</a>
                                <a href="<?php echo admin_url('admin-post.php?action=delete_tag&tag_id=' . $tag_group->term_id); ?>" class="button delete-tag-item-button">Delete</a>
                            </td>
                        </tr>
                    <?php }
                } else { ?>
                    <tr>
                        <td colspan="6">No tags found.</td>
                    </tr>
                <?php } ?>
            </tbody>
        </table>


    </div>
<?php
}


function tag_admin_menu()
{
    add_submenu_page(null, 'View/Edit Car Model', 'View/Edit Car Model', 'manage_options', 'view-edit-tag', 'tag_view_edit_page_callback');
}
add_action('admin_menu', 'tag_admin_menu');




// Add New Tag Page Callback
function add_new_tag_page_callback()
{
?>
    <div class="wrap">
        <h1>Add New Tag</h1>
        <form method="post" action="<?php echo admin_url('admin-post.php'); ?>">
            <input type="hidden" name="action" value="save_new_tag">


            <!-- Tag Type Input -->
            <div style="margin-bottom: 10px;margin-bottom: 10px;display: flex;justify-content: space-between;width: 20%;align-items: center;">
                <label for="tag-type">Tag Type</label>
                <input type="text" name="tag_type" required>
            </div>


            <!-- Tag Group Input -->
            <div style="margin-bottom: 10px;margin-bottom: 10px;display: flex;justify-content: space-between;width: 20%;align-items: center;">
                <label for="tag-group">Tag Group</label>
                <input type="text" name="tag_group" required>
            </div>
            <div style="margin-bottom: 10px;margin-bottom: 10px;display: flex;justify-content: space-between;width: 20%;align-items: center;">
                <label for="tag-group">Sort</label>
                <input type="number" name="sort" required>
            </div>
            <div style="margin-bottom: 10px;margin-bottom: 10px;display: flex;justify-content: space-between;width: 20%;align-items: center;">
                <label for="tag-group">State</label>
                <input type="number" name="state" required>
            </div>
            <!-- Tag Name with Add/Delete Icons -->
            <div id="tag-names-container">
				<label>Tag Name</label>
                <div class="tag-name-row" style="margin-bottom: 10px;">
                    <input type="text" name="tag_names[]" placeholder="Tag Name">
                    <button type="button" class="button remove-tag-button" style="margin-left: 10px;">Delete</button>
                </div>
            </div>
            <button type="button" id="add-tag-name" class="button button-secondary" style="margin-top: 10px;">+ Add Tag Name</button>


            <br><br>
            <input type="submit" class="button button-primary" value="Save Tag">
        </form>
    </div>


    <script>
        document.getElementById('add-tag-name').addEventListener('click', function() {
            const container = document.getElementById('tag-names-container');
            const row = document.createElement('div');
            row.classList.add('tag-name-row');
            row.innerHTML = '<input type="text" name="tag_names[]" placeholder="Tag Name">' +
                '<button type="button" class="button remove-tag-button" style="margin-left: 10px;">Delete</button>';
            container.appendChild(row);


            row.querySelector('.remove-tag-button').addEventListener('click', function() {
                row.remove();
            });
        });


        document.querySelectorAll('.remove-tag-button').forEach(button => {
            button.addEventListener('click', function() {
                button.closest('.tag-name-row').remove();
            });
        });
    </script>
    <?php
}


// Save New Tag Submission
function save_new_tag_submission()
{
    if (isset($_POST['tag_type'], $_POST['tag_group'], $_POST['tag_names'], $_POST['sort'], $_POST['state'])) {
        // Sanitize inputs
        $tag_group = sanitize_text_field($_POST['tag_group']);
        $tag_names = array_map('sanitize_text_field', $_POST['tag_names']);
        $sort = intval($_POST['sort']);
        $state = intval($_POST['state']);


        // Insert the tag group into the 'terms' table
        $tag_group_term = wp_insert_term(
            $tag_group,  // Tag Group Name
            'all-tags',  // Taxonomy
            [
                'description' => 'parent-tags',  // Group Description
                'parent' => 0,  // No parent for the main group
            ]
        );


        // Check if the tag group was inserted successfully
        if (!is_wp_error($tag_group_term)) {
            $tag_group_term_id = $tag_group_term['term_id'];


            // Debug: check tag group ID
            error_log('Tag Group Term ID: ' . $tag_group_term_id);


            // Insert meta for the tag group: sort and state
            update_term_meta($tag_group_term_id, 'sort', $sort);
            update_term_meta($tag_group_term_id, 'state', $state);


            // Now add the tag names (as sub-tags) under the tag group
            foreach ($tag_names as $tag_name) {
                $term_name = wp_insert_term(
                    $tag_name,  // Tag Name
                    'all-tags',  // Taxonomy
                    [
                        'parent' => $tag_group_term_id,  // Set the tag group as the parent
                        'description' => 'sub-tags',  // Sub-tag description
                    ]
                );


                // Debug: log the tag name and term ID
                if (!is_wp_error($term_name)) {
                    error_log('Sub Tag "' . $tag_name . '" Inserted with ID: ' . $term_name['term_id']);
                } else {
                    error_log('Error inserting sub tag "' . $tag_name . '": ' . $term_name->get_error_message());
                }
            }
        } else {
            // If there was an error inserting the tag group, log it
            error_log('Error inserting tag group: ' . $tag_group_term->get_error_message());
        }
    }

    wp_redirect(admin_url('admin.php?page=tag_manage'));
    exit;
}
add_action('admin_post_save_new_tag', 'save_new_tag_submission');


// View/Edit Tag Page Callback
function tag_view_edit_page_callback()
{
    if (isset($_GET['tag_id'])) {
        $tag_id = intval($_GET['tag_id']);
        $term = get_term($tag_id, 'all-tags');


        if (!is_wp_error($term)) {
            $is_edit_mode = isset($_GET['action']) && $_GET['action'] === 'edit'; // Check if it's in edit mode
            $tag_meta = get_term_meta($tag_id); // Get term meta data

            // Extract tag data from term and term meta
            $tag_type = !empty($tag_meta['tag_type'][0]) ? $tag_meta['tag_type'][0] : '';
            $tag_group = !empty($tag_meta['tag_group'][0]) ? $tag_meta['tag_group'][0] : '';
            $tag_names = !empty($tag_meta['tag_names']) ? $tag_meta['tag_names'] : []; // Assume it's stored as a serialized array

    ?>
            <div class="wrap">
                <h1><?php echo $is_edit_mode ? 'Edit Tag' : 'View Tag'; ?></h1>
                <form method="post" action="<?php echo admin_url('admin-post.php'); ?>">
                    <input type="hidden" name="action" value="save_edited_tag">
                    <input type="hidden" name="tag_id" value="<?php echo esc_attr($tag_id); ?>">


                    <!-- Tag Type Input -->
                    <div style="margin-bottom: 10px;margin-bottom: 10px;display: flex;justify-content: space-between;width: 20%;align-items: center;">
                        <label for="tag-type">Tag Type</label>
                        <input type="text" name="tag_type" value="<?php echo esc_attr($tag_type); ?>" required <?php echo $is_edit_mode ? '' : 'disabled'; ?>>
                    </div>


                    <!-- Tag Group Input -->
                    <div style="margin-bottom: 10px;margin-bottom: 10px;display: flex;justify-content: space-between;width: 20%;align-items: center;">
                        <label for="tag-group">Tag Group</label>
                        <input type="text" name="tag_group" value="<?php echo esc_attr($tag_group); ?>" required <?php echo $is_edit_mode ? '' : 'disabled'; ?>>
                    </div>


                    <!-- Tag Name with Add/Delete Icons -->
					<label>Tag Name</label>
                    <div id="tag-names-container" style="margin-top: 10px;">
                        <?php foreach ($tag_names as $name) { ?>
						
                            <div class="tag-name-row">
                                <input style="margin-top: 10px;" type="text" name="tag_names[]" value="<?php echo esc_attr($name); ?>" placeholder="Tag Name" <?php echo $is_edit_mode ? '' : 'disabled'; ?>>
                                <?php if ($is_edit_mode) { ?>
                                    <button type="button" class="button remove-tag-button" style="margin-left: 10px;">Delete</button>
                                <?php } ?>
                            </div>
                        <?php } ?>
                    </div>
                    <?php if ($is_edit_mode) { ?>
                        <button type="button" id="add-tag-name" class="button button-secondary" style="margin-top: 10px;">+ Add Tag Name</button>
                    <?php } ?>


                    <br><br>
                    <?php if ($is_edit_mode) { ?>
                        <input type="submit" class="button button-primary" value="Save Tag">
                    <?php } else { ?>
                        <a href="<?php echo admin_url('admin.php?page=tag_manage'); ?>" class="button">Back to Tag Manage</a>
                    <?php } ?>
                </form>
            </div>


            <script>
                // Add new tag name functionality
                <?php if ($is_edit_mode) { ?>
                    document.getElementById('add-tag-name').addEventListener('click', function() {
                        const container = document.getElementById('tag-names-container');
                        const row = document.createElement('div');
                        row.classList.add('tag-name-row');
                        row.innerHTML = '<input type="text" name="tag_names[]" placeholder="Tag Name">' +
                            '<button type="button" class="button remove-tag-button" style="margin-left: 10px;">Delete</button>';
                        container.appendChild(row);


                        row.querySelector('.remove-tag-button').addEventListener('click', function() {
                            row.remove();
                        });
                    });
                <?php } ?>


                document.querySelectorAll('.remove-tag-button').forEach(button => {
                    button.addEventListener('click', function() {
                        button.closest('.tag-name-row').remove();
                    });
                });
            </script>
<style>
	.tag-name-row{
		padding-bottom:10px !important;
	}
</style>
<?php
        } else {
            echo '<div class="error notice"><p>Tag not found.</p></div>';
        }
    }
}


// Save Edited Tag Submission
// Save Edited Tag Submission (Modified)
function save_edited_tag_submission()
{
    if (isset($_POST['tag_id'], $_POST['tag_type'], $_POST['tag_group'], $_POST['tag_names'], $_POST['sort'], $_POST['state'])) {
        $tag_id = intval($_POST['tag_id']);
        global $wpdb;


        // Update the tag group and meta fields
        wp_update_term($tag_id, 'all-tags', [
            'name' => sanitize_text_field($_POST['tag_group']),
            'description' => 'parent-tags'
        ]);


        update_term_meta($tag_id, 'sort', sanitize_text_field($_POST['sort']));
        update_term_meta($tag_id, 'state', sanitize_text_field($_POST['state']));


        // Update tag names (children tags)
        $tag_names = array_map('sanitize_text_field', $_POST['tag_names']);
        foreach ($tag_names as $tag_name) {
            wp_insert_term(
                $tag_name,
                'all-tags',
                [
                    'parent' => $tag_id,
                    'description' => 'sub-tags'
                ]
            );
        }
    }


    wp_redirect(admin_url('admin.php?page=tag_manage'));
    exit;
}
add_action('admin_post_save_edited_tag', 'save_edited_tag_submission');


// Delete Tag Submission
// Delete Tag Submission (Modified)
function delete_tag_submission()
{
    if (isset($_GET['tag_id'])) {
        $tag_id = intval($_GET['tag_id']);


        // Delete the tag group and its associated meta and taxonomy entries
        wp_delete_term($tag_id, 'all-tags');
    }


    wp_redirect(admin_url('admin.php?page=tag_manage'));
    exit;
}
add_action('admin_post_delete_tag', 'delete_tag_submission');
