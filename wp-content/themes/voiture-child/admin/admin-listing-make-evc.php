<?php
function register_listing_make_evc_taxonomy() {
    $labels = [
        'name'              => _x('Makes EVC', 'taxonomy general name', 'textdomain'),
        'singular_name'     => _x('Make EVC', 'taxonomy singular name', 'textdomain'),
        'search_items'      => __('Search Makes EVC', 'textdomain'),
        'all_items'         => __('All Makes EVC', 'textdomain'),
        'edit_item'         => __('Edit Make EVC', 'textdomain'),
        'update_item'       => __('Update Make EVC', 'textdomain'),
        'add_new_item'      => __('Add New Make EVC', 'textdomain'),
        'new_item_name'     => __('New Make EVC Name', 'textdomain'),
        'menu_name'         => __('Makes EVC', 'textdomain'),
    ];

    $args = [
        // 'hierarchical'      => true,
        'labels'            => $labels,
        'show_ui'           => true,
        'show_admin_column' => true,
        'query_var'         => true,
        'rewrite'           => ['slug' => 'listing-make-evc'],
    ];

    register_taxonomy('listing_make_evc', ['post'], $args);
}
add_action('init', 'register_listing_make_evc_taxonomy');

/**
 * Add Sort Order and Name Meta Fields to Make EVC Taxonomy
 */
// Add meta fields (Sort Order and Name Select)
function listing_make_evc_add_meta_field() {
    ?>
	<div class="form-field">
        <label for="term_name_select"><?php _e('Name', 'textdomain'); ?></label>
        <select name="term_name_select" id="term_name_select">
            <option value=""><?php _e('Select Make', 'textdomain'); ?></option>
            <?php
            $makes = get_terms([
                'taxonomy' => 'listing_make',
                'hide_empty' => false,
            ]);
            foreach ($makes as $make) {
                echo '<option value="' . esc_attr($make->term_id) . '">' . esc_html($make->name) . '</option>';
            }
            ?>
        </select>
		<input type="hidden" name="tag-name" id="tag-name" value="">
        <p class="description"><?php _e('Select a make from the existing makes.', 'textdomain'); ?></p>
    </div>
    <div class="form-field">
        <label for="term_sort_order"><?php _e('Sort Order', 'textdomain'); ?></label>
        <input type="number" name="sort" id="term_sort_order" value="" min="0">
        <p class="description"><?php _e('Set the sort order for this make.', 'textdomain'); ?></p>
    </div>
    
	<script type="text/javascript">
        // Update the hidden input field with the selected make name
        document.getElementById('term_name_select').addEventListener('change', function() {
            var selectedOption = this.options[this.selectedIndex];
            var makeName = selectedOption.text;
            document.getElementById('tag-name').value = makeName;
        });
    </script>

    <?php
}
add_action('listing_make_evc_add_form_fields', 'listing_make_evc_add_meta_field');

// Edit meta fields (Sort Order and Name Select)
function listing_make_evc_edit_meta_field($term) {
    $sort_order = get_term_meta($term->term_id, 'sort', true);
    $parent_id = $term->parent; // The parent term ID of the current term
    $selected_name = $parent_id; // Use parent term ID to pre-select the make
    ?>
	<tr class="form-field">
        <th scope="row">
            <label for="term_name_select"><?php _e('Name', 'textdomain'); ?></label>
        </th>
        <td>
            <select name="term_name_select" id="term_name_select">
                <option value=""><?php _e('Select Make', 'textdomain'); ?></option>
                <?php
                $makes = get_terms([
                    'taxonomy' => 'listing_make',
                    'hide_empty' => false,
                ]);
                foreach ($makes as $make) {
                    $selected = selected($selected_name, $make->term_id, false);
                    echo '<option value="' . esc_attr($make->term_id) . '" ' . $selected . '>' . esc_html($make->name) . '</option>';
                }
                ?>
            </select>
			<input type="hidden" name="tag-name" id="tag-name" value="<?php echo esc_attr($term->name); ?>">
            <p class="description"><?php _e('Select a make from the existing makes.', 'textdomain'); ?></p>
        </td>
    </tr>
    <tr class="form-field">
        <th scope="row">
            <label for="term_sort_order"><?php _e('Sort Order', 'textdomain'); ?></label>
        </th>
        <td>
            <input type="number" name="sort" id="term_sort_order" value="<?php echo esc_attr($sort_order); ?>" min="0">
            <p class="description"><?php _e('Set the sort order for this make.', 'textdomain'); ?></p>
        </td>
    </tr>
    <script type="text/javascript">
        // Update the hidden input field with the selected make name
        document.getElementById('term_name_select').addEventListener('change', function() {
            var selectedOption = this.options[this.selectedIndex];
            var makeName = selectedOption.text;
            document.getElementById('tag-name').value = makeName;
        });
    </script>
    <?php
}
add_action('listing_make_evc_edit_form_fields', 'listing_make_evc_edit_meta_field');

//save data
function save_listing_make_evc_meta($term_id) {
    if (!isset($_POST['term_name_select']) || empty($_POST['term_name_select'])) {
        return;
    }

    if (isset($_POST['sort'])) {
        $sort_order = intval($_POST['sort']);
        update_term_meta($term_id, 'sort', $sort_order);
    }

    $make_id = intval($_POST['term_name_select']);

    $selected_term = get_term($make_id, 'listing_make'); // Specify taxonomy
    if (!$selected_term || is_wp_error($selected_term)) {
        return; // Exit early if there's an error
    }

    $term_name = $selected_term->name;
    $updated_term = wp_update_term($term_id, 'listing_make_evc', [
    'name' => $term_name,
    'parent' => $make_id
	]);

	if (is_wp_error($updated_term)) {
		error_log('Error updating term: ' . $updated_term->get_error_message());
	} else {
		error_log('Term updated successfully, redirecting...');
	}
    wp_redirect(admin_url('edit-tags.php?taxonomy=listing_make_evc'));
    exit;
}
add_action('edited_listing_make_evc', 'save_listing_make_evc_meta', 10, 3);
add_action('create_listing_make_evc', 'save_listing_make_evc_meta', 10, 3);

/**
 * Populate Sort Order and Name Columns
 */
function listing_make_evc_custom_column_content($content, $column_name, $term_id) {
    if ($column_name === 'sort_order') {
        $content = get_term_meta($term_id, 'sort', true);
    } elseif ($column_name === 'name_select') {
        $selected_name_id = get_term_meta($term_id, 'name', true);
        if ($selected_name_id) {
            $term = get_term($selected_name_id);
            $content = $term ? esc_html($term->name) : '';
        }
    }
    return $content;
}
add_filter('manage_listing_make_evc_custom_column', 'listing_make_evc_custom_column_content', 10, 3);

function remove_taxonomy_fields_in_admin() {
    // Check if we are on the taxonomy edit screen for 'listing_make_evc'
    if ( isset( $_GET['taxonomy'] ) && $_GET['taxonomy'] === 'listing_make_evc' ) {
        ?>
        <script type="text/javascript">
            document.addEventListener('DOMContentLoaded', function() {
                // Remove the Name, Slug, and Description fields in the taxonomy term form
                var nameField = document.querySelector('.term-name-wrap');
                var slugField = document.querySelector('.term-slug-wrap');
                var descriptionField = document.querySelector('.term-description-wrap');
                
                if (nameField) nameField.remove();
                if (slugField) slugField.remove();
                if (descriptionField) descriptionField.remove();
            });
        </script>
        <?php
    }
}
add_action( 'admin_head', 'remove_taxonomy_fields_in_admin' );


function add_listing_make_evc_submenu() {
    // Check if the custom taxonomy is registered
    if ( taxonomy_exists('listing_make_evc') ) {
        // Add the submenu under the 'listing' menu
        add_submenu_page(
            'edit.php?post_type=listing',      // Parent menu slug (this is the slug for the 'listing' post type)
            'Listing Make EVC',                       // Page title
            'Listing Make EVC',                       // Menu title
            'manage_options',                  // Capability required to view this menu
            'edit-tags.php?taxonomy=listing_make_evc&post_type=listing', // Submenu URL (taxonomy slug + post type)
            null                               // No callback function required, it's handled by WordPress
        );
    }
}
add_action('admin_menu', 'add_listing_make_evc_submenu');

// Remove the Description and Slug columns in the listing taxonomy
function custom_listing_make_evc_columns($columns) {
    // Unset the 'description' and 'slug' columns
    unset($columns['description']);
    unset($columns['slug']);
    
    return $columns;
}
add_filter('manage_edit-listing_make_evc_columns', 'custom_listing_make_evc_columns');
