<?php
function car_gallery_menu()
{
    add_menu_page(
        'Gallery',  // Page title
        'Gallery',        // Menu title
        'manage_options',     // Capability
        'gallery',        // Menu slug
        'display_car_gallery_page', // Callback function to display content
        'dashicons-format-gallery', // Icon
        6                     // Menu position
    );

    add_submenu_page(
        'gallery',
        'Add Gallery',
        'Add Gallery',
        'manage_options',
        'add_new_gallery',
        'add_new_gallery_page_callback'
    );
}
add_action('admin_menu', 'car_gallery_menu');

function display_car_gallery_page()
{
    global $wpdb;

    // Set items per page and get the current page number
    $items_per_page = 10;
    $current_page = isset($_GET['paged']) ? max(1, intval($_GET['paged'])) : 1;

    // Calculate offset and get the total number of items
    $offset = ($current_page - 1) * $items_per_page;
    $total_items = $wpdb->get_var("SELECT COUNT(DISTINCT variant_post_id) FROM car_image");

    // Calculate total pages
    $total_pages = ceil($total_items / $items_per_page);

    // Retrieve the variants for the current page
    $variant_results = $wpdb->get_results($wpdb->prepare(
        "SELECT DISTINCT variant_post_id FROM car_image ORDER BY create_time DESC LIMIT %d OFFSET %d",
        $items_per_page,
        $offset
    ));

    if (isset($_GET['deleted'])) {
        if ($_GET['deleted'] === 'success') {
            echo '<div class="updated"><p>Gallery item deleted successfully.</p></div>';
        } elseif ($_GET['deleted'] === 'error') {
            echo '<div class="error"><p>Error occurred while deleting the gallery item.</p></div>';
        } elseif ($_GET['deleted'] === 'invalid') {
            echo '<div class="error"><p>Invalid gallery item ID provided.</p></div>';
        }
    }

?>
    <div class="wrap">
        <h1>Gallery</h1>
        <!-- Add New Tag Button -->
        <a href="<?php echo admin_url('admin.php?page=add_new_gallery'); ?>" class="button button-primary" style=" margin-left: 1200px">+ Add New</a>

        <table class="widefat fixed" style="width: 100%; margin-bottom: 20px; margin-top: 20px;">
            <thead class="text-center">
                <tr>
                    <th style="width: 100px;">Variant</th>
                    <th style="width: 100px;">Color</th>
                    <th style="width: 100px;">Type</th>
                    <th>Images</th>
                    <th style="width: 200px;">Actions</th>
                </tr>
            </thead>
            <tbody>
                <?php if (!empty($variant_results)) {
                    foreach ($variant_results as $variant_row) {
                        // Fetch variant title
                        $variant_data = $wpdb->get_row($wpdb->prepare("SELECT post_title FROM wp_posts WHERE ID = %d", $variant_row->variant_post_id));
                        $variant_title = $variant_data ? $variant_data->post_title : '--';

                        // Fetch color, sort, and types for each variant
                        $variant_details = $wpdb->get_results($wpdb->prepare(
                            "SELECT * FROM car_image WHERE variant_post_id = %d ORDER BY type, sort",
                            $variant_row->variant_post_id
                        ));

                        if (!empty($variant_details)) {
                            $first_row = $variant_details[0];
                ?>
                            <tr>
                                <td rowspan="<?php echo count($variant_details); ?>" class="text-center"><?php echo esc_html($variant_title); ?></td>
                                <td rowspan="<?php echo count($variant_details); ?>" class="text-center"><?php echo esc_html($first_row->colour); ?></td>
                                <?php
                                foreach ($variant_details as $index => $detail) {
                                    if ($index > 0) echo '<tr>'; // Add a new row for subsequent types
                                ?>
                                    <td class="text-center"><?php echo esc_html($detail->type); ?></td>
                                    <td>
                                        <?php
                                        $images = json_decode($detail->image_data);
                                        if (!empty($images)) :
                                            foreach ($images as $image) :
                                        ?>
                                                <a href="<?php echo esc_url($image->url); ?>" target="_blank">
                                                    <img src="<?php echo esc_url($image->url); ?>" alt="Image" style="width: 50px; height: 50px; margin-right: 5px;" />
                                                </a>
                                            <?php
                                            endforeach;
                                        else :
                                            ?>
                                            <span>No images available</span>
                                        <?php endif; ?>
                                    </td>

                                    <?php if ($index == 0) : ?>
                                        <td rowspan="<?php echo count($variant_details); ?>" class="text-center">
                                            <a href="<?php echo admin_url('admin.php?page=view-edit-gallery&variant_id=' . $variant_row->variant_post_id . '&action=edit'); ?>" class="button">Edit</a>
                                            <a href="<?php echo admin_url('admin.php?page=view-edit-gallery&variant_id=' . $variant_row->variant_post_id); ?>" class="button">View</a>
                                            <a href="<?php echo admin_url('admin-post.php?action=delete_gallery&variant_post_id=' . $variant_row->variant_post_id . '&nonce=' . wp_create_nonce('delete_gallery_nonce')); ?>" class="button delete-tag-item-button" onclick="return confirm('Are you sure you want to delete this item?');">Delete</a>
                                        </td>
                                    <?php endif; ?>
                                <?php
                                    if ($index > 0) echo '</tr>'; // Close the row for each type and image
                                } ?>
                            </tr>

                    <?php
                        }
                    }
                } else { ?>
                    <tr>
                        <td colspan="6">No data found.</td>
                    </tr>
                <?php } ?>
            </tbody>
        </table>

        <!-- Pagination -->
        <div class="tablenav-pages" style="text-align: right; margin-top: 20px;">
            <?php
            $pagination_base = admin_url('admin.php?page=gallery%_%');
            $pagination_format = '&paged=%#%';
            echo paginate_links(array(
                'base' => $pagination_base,
                'format' => $pagination_format,
                'current' => $current_page,
                'total' => $total_pages,
                'prev_text' => '<span class="button">« Prev</span>',
                'next_text' => '<span class="button">Next »</span>',
                'before_page_number' => '<span class="button">',
                'after_page_number' => '</span>',
            ));
            ?>
        </div>
    </div>
    <style>
        /* Pagination container alignment */
        .tablenav-pages {
            display: flex;
            justify-content: flex-end;
            /* Aligns pagination to the right */
        }

        /* Style each pagination link */
        .tablenav-pages a,
        .tablenav-pages span {
            padding: 8px 12px;
            margin: 0 4px;
            background-color: #f1f1f1;
            color: #007cba;
            text-decoration: none;
            border-radius: 4px;
            cursor: pointer;
        }

        /* Style for active pagination link */
        .tablenav-pages .current {
            background-color: #007cba;
            color: #fff;
            font-weight: bold;
            pointer-events: none;
        }

        /* Hover effect for pagination links */
        .tablenav-pages a:hover {
            background-color: #005a9c;
            color: #fff;
        }

        /* Add borders to table cells for horizontal and vertical lines */
        table.widefat.fixed {
            border-collapse: collapse;
            width: 100%;
        }

        /* Border for table headers */
        table.widefat.fixed th {
            border: 1px solid #ddd;
            padding: 10px;
            background-color: #f9f9f9;
        }

        /* Border for table cells */
        table.widefat.fixed td {
            border: 1px solid #ddd;
            padding: 10px;
        }

        .text-center {
            align-content: center;
            text-align: center;
        }
    </style>
<?php
}

function add_new_gallery_page_callback()
{
    global $wpdb;
    $existing_variants = $wpdb->get_col("SELECT variant_post_id FROM car_image");
?>
    <div class="wrap">
        <h1>Add New Gallery</h1>
        <form method="post" action="<?php echo admin_url('admin-post.php'); ?>" style="max-width: 600px;">
            <input type="hidden" name="action" value="add_gallery">

            <!-- Variant Select Field -->
            <div class="form-group">
                <label for="variant">Variant:</label>
                <select name="variant_post_id" required style="max-width:40rem; border: 1px solid #ccc">
                    <?php
                    global $wpdb;
                    $variants = $wpdb->get_results("SELECT ID, post_title FROM wp_posts WHERE post_type = 'variant'");
                    echo '<option value="">Select Variant </option>';
                    foreach ($variants as $variant) {
                        // Disable the option if it exists in the existing variants
                        $disabled = in_array($variant->ID, $existing_variants) ? 'disabled' : '';
                        echo '<option value="' . esc_attr($variant->ID) . '" ' . $disabled . '>' . esc_html($variant->post_title) . '</option>';
                    }
                    ?>
                </select>
            </div>

            <!-- Color Field -->
            <div class="form-group">
                <label for="color">Color:</label>
                <input type="text" name="color" placeholder="Enter Color code (e.g. #fff)" required>
            </div>

            <!-- Sort Field -->
            <div class="form-group">
                <label for="sort">Sort Order:</label>
                <input type="number" name="sort" min="1" required>
            </div>

            <!-- Image Upload Fields for Each Type -->
         <div class="gallery-form-group-con">
			    <div class="form-group">
                <label for="exterior_images">Upload Exterior Images (Max 30):</label>
                <div id="exterior-images"></div>
                <button type="button" class="button upload-images-button" data-type="exterior">Upload Exterior Images</button>
                <input type="hidden" name="exterior_images" id="exterior-images-input" value="">
            </div>

            <div class="form-group">
                <label for="interior_images">Upload Interior Images (Max 30):</label>
                <div id="interior-images"></div>
                <button type="button" class="button upload-images-button" data-type="interior">Upload Interior Images</button>
                <input type="hidden" name="interior_images" id="interior-images-input" value="">
            </div>

            <div class="form-group">
                <label for="other_images">Upload Other Images (Max 30):</label>
                <div id="others-images"></div>
                <button type="button" class="button upload-images-button" data-type="others">Upload Other Images</button>
                <input type="hidden" name="others_images" id="others-images-input" value="">
            </div>
			</div>

			<div class="admin-gallery-btn-con">
	         <div class="save-gallery-btn">
			    <?php submit_button('Save Gallery'); ?>
			</div>
            <div class="back-to-gallery-btn">
               <a href="<?php echo admin_url('admin.php?page=gallery'); ?>" class="button">Back to Gallery</a>
			</div>
			</div>
        </form>
    </div>
    <script>
        jQuery(document).ready(function($) {
            var imageFrame;

            $('.upload-images-button').on('click', function(e) {
                e.preventDefault();
                var imageType = $(this).data('type');
                var inputField = $('#' + imageType + '-images-input');
                var displayArea = $('#' + imageType + '-images');

                wp.media.model.settings.multiple = true;

                imageFrame = wp.media({
                    title: 'Select Images',
                    button: {
                        text: 'Add Images'
                    },
                    multiple: true
                });

                imageFrame.on('select', function() {
                    var selection = imageFrame.state().get('selection');
                    var validImages = [];
                    var errorMessages = [];

                    selection.each(function(attachment) {
                        attachment = attachment.toJSON();
                        if (attachment.filesizeInBytes <= 100 * 1024) {
                            validImages.push(attachment.url);
                        } else {
                            errorMessages.push(attachment.title + ' is too large (max 100KB allowed).');
                        }
                    });

                    // Show error messages
                    if (errorMessages.length > 0) {
                        alert(errorMessages.join("\n"));
                    }

                    // Limit to a maximum of 30 valid images
                    if (validImages.length > 30) {
                        validImages = validImages.slice(0, 30);
                        alert('Only the first 30 valid images will be added.');
                    }

                    inputField.val(validImages.join(','));
                    displayArea.html(validImages.map(function(url) {
                        return '<img id="gallery-image" src="' + url + '" alt="Gallery Image">';
                    }).join(''));
                });

                imageFrame.open();
            });
        });
    </script>
    <style>
		.save-gallery-btn p{
			margin-top:0px !important;
			padding:0px !important;
		}
		.admin-gallery-btn-con{
			    display: flex;
				justify-content: center;
				gap: 30px;
				margin-top: 30px;
		}
		.gallery-form-group-con{
			display:flex;
			gap:30px;
		}
        .form-group {
            margin-bottom: 15px;
            /* Space between input fields */
        }

        label {
            display: block;
            /* Makes label occupy full width */
            margin-bottom: 5px;
            /* Space between label and input */
            font-weight: bold;
			margin-bottom:5px;
            /* Bold labels */
        }

        input[type="text"],
        input[type="number"],
        select {
            width: 100%;
            border: 1px solid #ccc;
            border-radius: 4px;
            box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.1);
            font-size: 14px;
			height:40px;
        }

        input[type="text"]:focus,
        input[type="number"]:focus,
        select:focus {
            border-color: #0073aa;
            /* Border color on focus */
            outline: none;
            /* Remove default outline */
        }

        button.button {
            margin-top: 5px;
            /* Space above button */
        }

        #gallery-images img {
            width: 50px;
            /* Width of uploaded images */
            margin-right: 5px;
            /* Space between images */
            vertical-align: middle;
            /* Align images vertically */
        }

        #exterior-image,
        #interior-image,
        #others-image {
            display: flex;
        }

        #gallery-image {
			width: 15%;
			height: 15%;
			margin-bottom: 10px;
			margin-left: 10px;
        }
		.upload-images-button{
			width:170px;
		}
    </style>
<?php
}

// Save Gallery Data
add_action('admin_post_add_gallery', 'save_gallery_data');
function save_gallery_data()
{
    global $wpdb;

    // Sanitize and prepare data
    $variant_post_id = intval($_POST['variant_post_id']);
    $color = sanitize_text_field($_POST['color']);
    $sort = intval($_POST['sort']);
    $types = ['exterior', 'interior', 'others'];

    foreach ($types as $type) {
        // Check if image data is provided
        if (!empty($_POST[$type . '_images'])) {
            $image_data = sanitize_text_field($_POST[$type . '_images']);
            $image_urls = array_filter(array_map('esc_url', explode(',', $image_data))); // Split and sanitize URLs

            // Format image data as JSON objects with "name" and "url"
            $formatted_images = [];
            foreach ($image_urls as $url) {
                $image_name = basename($url);
                $formatted_images[] = [
                    'name' => "file/" . $image_name,
                    'url' => $url,
                ];
            }

            // Convert formatted images array to JSON string
            $json_image_data = json_encode($formatted_images);

            // Insert image data
            $result = $wpdb->insert('car_image', [
                'variant_id' => 0,
                'variant_post_id' => $variant_post_id,
                'colour' => $color,
                'type' => ucfirst($type),
                'sort' => $sort,
                'image_data' => $json_image_data,
                'create_time' => current_time('mysql'),
                'update_time' => current_time('mysql')
            ]);

            // Check for database errors
            if (!$result) {
                error_log("Database Insert Error for $type: " . $wpdb->last_error);
            } else {
                error_log("Data successfully inserted for $type.");
            }
        } else {
            error_log("No images provided for type: $type");
        }
    }

    // Redirect after saving
    wp_redirect(admin_url('admin.php?page=gallery'));
    exit;
}

function gallery_admin_menu()
{
    add_submenu_page(null, 'View/Edit Car Model', 'View/Edit Car Model', 'manage_options', 'view-edit-gallery', 'view_edit_gallery_item');
}
add_action('admin_menu', 'gallery_admin_menu');

function view_edit_gallery_item()
{
    global $wpdb;

    $variant_post_id = isset($_GET['variant_id']) ? intval($_GET['variant_id']) : 0; // Use variant_post_id
    $action = isset($_GET['action']) ? $_GET['action'] : 'view'; // Default to 'view'

    // Fetch the existing gallery item details using variant_post_id
    $rows = $wpdb->get_results($wpdb->prepare("SELECT * FROM car_image WHERE variant_post_id = %d", $variant_post_id));

    if (!$rows) {
        echo '<div class="wrap"><h2>Gallery Item Not Found</h2></div>';
        return;
    }

    $exterior_images = [];
    $interior_images = [];
    $other_images = [];

    foreach ($rows as $row) {
        $image_data = json_decode($row->image_data, true);

        if ($row->type === 'Exterior') {
            $exterior_images = array_merge($exterior_images, $image_data);
        } elseif ($row->type === 'Interior') {
            $interior_images = array_merge($interior_images, $image_data);
        } elseif ($row->type === 'Others') {
            $other_images = array_merge($other_images, $image_data);
        }
    }

    $existing_variants = $wpdb->get_col("SELECT variant_post_id FROM car_image");
?>
    <div class="wrap">
        <h1><?php echo esc_html($action === 'edit' ? 'Edit Gallery Item' : 'View Gallery Item'); ?></h1>
        <form method="post" action="<?php echo admin_url('admin-post.php'); ?>" style="max-width: 600px;">
            <input type="hidden" name="action" value="<?php echo esc_attr($action === 'edit' ? 'edit_gallery' : ''); ?>">
            <input type="hidden" name="variant_post_id" value="<?php echo esc_attr($variant_post_id); ?>">

            <div class="form-group">
                <label for="variant">Variant:</label>
                <select name="variant_post_id" required style="max-width: 40rem; border: 1px solid #ccc" <?php echo $action === 'view' ? 'disabled' : ''; ?>>
                    <?php
                    $variants = $wpdb->get_results("SELECT ID, post_title FROM wp_posts WHERE post_type = 'variant'");
                    foreach ($variants as $variant) {
                        // Disable the option if it exists in the existing variants
                        $disabled = in_array($variant->ID, $existing_variants) ? 'disabled' : '';
                        echo '<option value="' . esc_attr($variant->ID) . '" ' . selected($row->variant_post_id, $variant->ID, false) . ' ' . $disabled . '>' . esc_html($variant->post_title) . '</option>';
                    }
                    ?>
                </select>
            </div>

            <div class="form-group">
                <label for="color">Color:</label>
                <input type="text" name="color" value="<?php echo esc_attr($row->colour); ?>" <?php echo $action === 'view' ? 'disabled' : ''; ?> required>
            </div>

            <div class="form-group">
                <label for="sort">Sort Order:</label>
                <input type="number" name="sort" value="<?php echo esc_attr($row->sort); ?>" min="1" <?php echo $action === 'view' ? 'disabled' : ''; ?> required>
            </div>

            <div class="form-group">
                <label>Exterior Images:</label>
                <div id="exterior-images">
                    <?php foreach ($exterior_images as $image) {
                        echo '<img src="' . esc_url($image['url']) . '" style="width: 50px; margin-right: 5px;" alt="Exterior Image">';
                    } ?>
                </div>
                <button type="button" class="button upload-images-button" data-type="exterior" <?php echo $action === 'view' ? 'disabled' : ''; ?>>Upload Exterior Images</button>
                <input type="hidden" name="exterior_images" id="exterior-images-input" value="">
            </div>

            <div class="form-group">
                <label>Interior Images:</label>
                <div id="interior-images">
                    <?php foreach ($interior_images as $image) {
                        echo '<img src="' . esc_url($image['url']) . '" style="width: 50px; margin-right: 5px;" alt="Interior Image">';
                    } ?>
                </div>
                <button type="button" class="button upload-images-button" data-type="interior" <?php echo $action === 'view' ? 'disabled' : ''; ?>>Upload Interior Images</button>
                <input type="hidden" name="interior_images" id="interior-images-input" value="">
            </div>

            <div class="form-group">
                <label>Other Images:</label>
                <div id="others-images">
                    <?php foreach ($other_images as $image) {
                        echo '<img src="' . esc_url($image['url']) . '" style="width: 50px; margin-right: 5px;" alt="Other Image">';
                    } ?>
                </div>
                <button type="button" class="button upload-images-button" data-type="others" <?php echo $action === 'view' ? 'disabled' : ''; ?>>Upload Other Images</button>
                <input type="hidden" name="others_images" id="others-images-input" value="">
            </div>

            <?php if ($action === 'edit') {
                submit_button('Update Gallery');
            } ?>
            <a href="<?php echo admin_url('admin.php?page=gallery'); ?>" class="button" style="margin-top: 10px;">Back to Gallery</a>
        </form>
    </div>
    <script>
        jQuery(document).ready(function($) {
            var imageFrame;

            $('.upload-images-button').on('click', function(e) {
                e.preventDefault();
                var imageType = $(this).data('type');
                var inputField = $('#' + imageType + '-images-input');
                var displayArea = $('#' + imageType + '-images');

                wp.media.model.settings.multiple = true;

                imageFrame = wp.media({
                    title: 'Select Images',
                    button: {
                        text: 'Add Images'
                    },
                    multiple: true
                });

                imageFrame.on('select', function() {
                    var selection = imageFrame.state().get('selection');
                    var validImages = [];

                    selection.each(function(attachment) {
                        attachment = attachment.toJSON();
                        if (attachment.filesizeInBytes <= 100 * 1024) { // Limit file size
                            validImages.push(attachment.url);
                        }
                    });

                    inputField.val(validImages.join(','));
                    displayArea.html(validImages.map(function(url) {
                        return '<img src="' + url + '" style="width: 50px; margin-right: 5px;" alt="Gallery Image">';
                    }).join(''));
                });

                imageFrame.open();
            });
        });
    </script>
    <style>
        .form-group {
            margin-bottom: 15px;
            /* Space between input fields */
        }

        label {
            display: block;
            /* Makes label occupy full width */
            margin-bottom: 5px;
            /* Space between label and input */
            font-weight: bold;
            /* Bold labels */
        }

        input[type="text"],
        input[type="number"],
        select {
            width: 100%;
            border: 1px solid #ccc;
            border-radius: 4px;
            box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.1);
            font-size: 14px;
        }

        input[type="text"]:focus,
        input[type="number"]:focus,
        select:focus {
            border-color: #0073aa;
            /* Border color on focus */
            outline: none;
            /* Remove default outline */
        }

        button.button {
            margin-top: 5px;
            /* Space above button */
        }

        #gallery-images img {
            width: 50px;
            /* Width of uploaded images */
            margin-right: 5px;
            /* Space between images */
            vertical-align: middle;
            /* Align images vertically */
        }

        #exterior-image,
        #interior-image,
        #others-image {
            display: flex;
        }

        #gallery-image {
            width: 10%;
            height: 10%;
        }
    </style>
<?php
}

// Handle form submission for edits
add_action('admin_post_edit_gallery', 'edit_gallery_data');

function edit_gallery_data()
{
    global $wpdb;

    $variant_post_id = intval($_POST['variant_post_id']);
    $color = sanitize_text_field($_POST['color']);
    $sort = intval($_POST['sort']);
    $types = ['exterior', 'interior', 'others'];

    foreach ($types as $type) {
        $images = isset($_POST[$type . '_images']) ? array_map('esc_url', explode(',', sanitize_text_field($_POST[$type . '_images']))) : [];

        if (!empty($images)) {
            $formatted_images = array_map(function ($url) {
                return [
                    'name' => 'file/' . basename($url),
                    'url' => $url,
                ];
            }, $images);

            // Update or insert image data by type
            $existing_record = $wpdb->get_row($wpdb->prepare(
                "SELECT * FROM car_image WHERE variant_post_id = %d AND type = %s",
                $variant_post_id,
                ucfirst($type)
            ));

            if ($existing_record) {
                // Update the record for this type
                $wpdb->update('car_image', [
                    'image_data' => json_encode($formatted_images),
                    'update_time' => current_time('mysql')
                ], [
                    'variant_post_id' => $variant_post_id,
                    'type' => ucfirst($type)
                ]);
            } else {
                // Insert new record if no existing record is found
                $wpdb->insert('car_image', [
                    'variant_post_id' => $variant_post_id,
                    'colour' => $color,
                    'type' => ucfirst($type),
                    'sort' => $sort,
                    'image_data' => json_encode($formatted_images),
                    'create_time' => current_time('mysql'),
                    'update_time' => current_time('mysql')
                ]);
            }
        }
    }

    // Redirect after editing
    wp_redirect(admin_url('admin.php?page=gallery'));
    exit;
}


// Hook for handling delete request
add_action('admin_post_delete_gallery', 'delete_gallery_item');

function delete_gallery_item()
{
    global $wpdb;

    // Check for nonce if you're using one for security
    // check_admin_referer('your_nonce_action'); // Uncomment and use if you have nonce

    if (isset($_GET['variant_post_id']) && is_numeric($_GET['variant_post_id'])) { // Use $_GET instead of $_POST
        $variant_post_id = intval($_GET['variant_post_id']); // Use the correct key

        // Perform the delete operation
        $deleted = $wpdb->delete('car_image', ['variant_post_id' => $variant_post_id]);

        // Redirect based on success or failure
        if ($deleted) {
            wp_redirect(admin_url('admin.php?page=gallery&deleted=success'));
            exit; // Always exit after wp_redirect
        } else {
            wp_redirect(admin_url('admin.php?page=gallery&deleted=error'));
            exit; // Always exit after wp_redirect
        }
    } else {
        wp_redirect(admin_url('admin.php?page=gallery&deleted=invalid'));
        exit; // Always exit after wp_redirect
    }
}
?>