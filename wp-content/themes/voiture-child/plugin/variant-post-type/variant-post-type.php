<?php
/*
Plugin Name: Variant Post Type
Plugin URI: http://yourwebsite.com/
Description: Adds a Variant post type under the Listing menu in the WP-admin dashboard.
Version: 1.0
Author: Revanth
Author URI: http://yourwebsite.com/
License: GPL2
*/

class VariantPostType {
    
    public function __construct() {
        add_action('init', [$this, 'register_variant_post_type']);
        add_action('admin_menu', [$this, 'move_variant_to_listing_menu']);
        // add_action('add_meta_boxes', [$this, 'add_variant_meta_boxes']);
        // add_action('save_post', [$this, 'save_variant_meta']);
    }

    // Register the Variant post type
    public function register_variant_post_type() {
        $labels = [
            'name'               => _x('Variants', 'post type general name', 'textdomain'),
            'singular_name'      => _x('Variant', 'post type singular name', 'textdomain'),
            'menu_name'          => _x('Variants', 'admin menu', 'textdomain'),
            'name_admin_bar'     => _x('Variant', 'add new on admin bar', 'textdomain'),
            'add_new'            => _x('Add New', 'variant', 'textdomain'),
            'add_new_item'       => __('Add New Variant', 'textdomain'),
            'new_item'           => __('New Variant', 'textdomain'),
            'edit_item'          => __('Edit Variant', 'textdomain'),
            'view_item'          => __('View Variant', 'textdomain'),
            'all_items'          => __('All Variants', 'textdomain'),
            'search_items'       => __('Search Variants', 'textdomain'),
            'parent_item_colon'  => __('Parent Variants:', 'textdomain'),
            'not_found'          => __('No variants found.', 'textdomain'),
            'not_found_in_trash' => __('No variants found in Trash.', 'textdomain'),
        ];

        $args = [
            'labels'             => $labels,
            'public'             => true,
            'publicly_queryable' => true,
            'show_ui'            => true,
            'show_in_menu'       => false, // Do not create a separate menu
            'query_var'          => true,
            'rewrite'            => ['slug' => 'variant'],
            'capability_type'    => 'post',
            'has_archive'        => true,
            'hierarchical'       => false,
            'menu_position'      => null,
            'supports'           => ['title', 'editor', 'thumbnail', 'excerpt'],
        ];

        register_post_type('variant', $args);
    }

    // Move Variant post type under Listing menu
    public function move_variant_to_listing_menu() {
        add_submenu_page(
            'edit.php?post_type=listing', // Parent slug
            __('Variants', 'textdomain'), // Page title
            __('Variants', 'textdomain'), // Menu title
            'manage_options',             // Capability
            'edit.php?post_type=variant'  // Menu slug
        );
    }

    // Add meta boxes for Variant Code and Image
    public function add_variant_meta_boxes() {
        add_meta_box(
            'variant_code_meta_box',           // ID
            __('Variant Code', 'textdomain'),  // Title
            [$this, 'render_variant_code_meta_box'],  // Callback function
            'variant',                         // Post type
            'normal',                          // Context
            'high'                             // Priority
        );

        add_meta_box(
            'variant_image_meta_box',          // ID
            __('Variant Image', 'textdomain'), // Title
            [$this, 'render_variant_image_meta_box'],  // Callback function
            'variant',                         // Post type
            'normal',                          // Context
            'high'                             // Priority
        );
    }

    // Render the Variant Code meta box
    public function render_variant_code_meta_box($post) {
        // Retrieve existing value from the database
        $variant_code = get_post_meta($post->ID, '_variant_code', true);
        echo '<label for="variant_code">' . __('Enter Variant Code:', 'textdomain') . '</label>';
        echo '<input type="text" id="variant_code" name="variant_code" value="' . esc_attr($variant_code) . '" style="width:100%;" />';
    }

    // Render the Variant Image meta box
    public function render_variant_image_meta_box($post) {
        // Retrieve existing value from the database
        $variant_image = get_post_meta($post->ID, '_variant_image', true);
        echo '<label for="variant_image">' . __('Upload Variant Image:', 'textdomain') . '</label>';
        echo '<input type="text" id="variant_image" name="variant_image" value="' . esc_attr($variant_image) . '" style="width:100%;" />';
        echo '<input type="button" id="upload_variant_image_button" class="button" value="' . __('Upload Image', 'textdomain') . '" />';
    }

    // Save the meta box data
    public function save_variant_meta($post_id) {
        // Verify nonce
        if (defined('DOING_AUTOSAVE') && DOING_AUTOSAVE) return;
        if (!current_user_can('edit_post', $post_id)) return;

        // Save Variant Code
        if (isset($_POST['variant_code'])) {
            update_post_meta($post_id, '_variant_code', sanitize_text_field($_POST['variant_code']));
        }

        // Save Variant Image
        if (isset($_POST['variant_image'])) {
            update_post_meta($post_id, '_variant_image', esc_url_raw($_POST['variant_image']));
        }
    }
}

// Initialize the class
new VariantPostType();

?>
