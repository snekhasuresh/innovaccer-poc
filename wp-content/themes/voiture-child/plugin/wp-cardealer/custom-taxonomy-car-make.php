<?php

if (! defined('ABSPATH')) {
    exit; // Exit if accessed directly
}

class Custom_Taxonomy_Car_Make extends WP_CarDealer_Taxonomy_Car_Make
{

    public static function init()
    {
        parent::init();
        remove_action("listing_make_add_form_fields", array(WP_CarDealer_Taxonomy_Car_Make::class, 'add_fields_form'));
        remove_action("listing_make_edit_form_fields", array(WP_CarDealer_Taxonomy_Car_Make::class, 'edit_fields_form'));

        add_filter("manage_edit-listing_make_columns", array(__CLASS__, 'tax_columns'));
        add_filter("manage_listing_make_custom_column", array(__CLASS__, 'tax_column'), 10, 3);
        add_action("listing_make_add_form_fields", array(__CLASS__, 'add_fields_form'));
        add_action("listing_make_edit_form_fields", array(__CLASS__, 'edit_fields_form'), 10, 2);
        add_action('create_term', array(__CLASS__, 'save'), 10, 3);
        add_action('edit_term', array(__CLASS__, 'save'), 10, 3);
    }

    public static function tax_columns($columns)
    {
        $columns = parent::tax_columns($columns);
        $columns['image'] = esc_html__('Image', 'wp-cardealer');
        return $columns;
    }

    public static function tax_column($columns, $column, $id)
    {
        $columns = parent::tax_column($columns, $column, $id);

        if ($column == 'image') {
            $image_url = get_term_meta($id, 'listing_make_image', true);
            if ($image_url) {
                $columns .= '<img src="' . esc_url($image_url) . '" alt="' . esc_attr__('Image', 'wp-cardealer') . '" style="max-width:50px;height:auto;" />';
            } else {
                $columns .= esc_html__('No image', 'wp-cardealer');
            }
        }

        return $columns;
    }

    public static function add_fields_form($taxonomy)
    {
?>
        <div class="form-field">
            <label for="sort"><?php esc_html_e('Sort Order', 'wp-cardealer'); ?></label>
            <input type="number" name="sort" id="sort" value="" />
            <p class="description"><?php esc_html_e('Enter a number to define the sort order for this make.', 'wp-cardealer'); ?></p>
        </div>

        <div class="form-field">
            <label><?php esc_html_e('Image', 'wp-cardealer'); ?></label>
            <div id="listing_make_image_preview" style="margin-bottom: 10px;">
                <img src="<?php echo esc_url(get_stylesheet_directory_uri() . '/plugin/wp-cardealer/images/placeholder.jpg'); ?>" alt="<?php esc_attr_e('Image', 'wp-cardealer'); ?>" style="max-width: 150px; height: auto;" />
            </div>
            <input type="hidden" name="listing_make_image" id="listing_make_image" value="" />
            <button type="button" class="upload_image_button button"><?php esc_html_e('Upload Image', 'wp-cardealer'); ?></button>
        </div>

        <div class="form-field">
            <label><?php esc_html_e('State', 'wp-cardealer'); ?></label>
                <div style="display:flex;gap: 30px;margin-top: 10px;">
	               <div style="  display: flex;align-items: center;">
			    <input type="radio" name="state" value="1" id="state_enable" checked/>
			    <label  style="margin-top: -5px;" for="state_enable"><?php esc_html_e('Enable', 'wp-cardealer'); ?></label>
			</div>
                <div style="  display: flex;align-items: center;">
			    <input type="radio" name="state" value="0" id="state_disable" />
				<label  style="margin-top: -5px;" for="state_disable"><?php esc_html_e('Disable', 'wp-cardealer'); ?></label>
			</div>
			</div>
            <p class="description"><?php esc_html_e('Select whether this term is enabled or disabled.', 'wp-cardealer'); ?></p>
        </div>

    <?php
    }

    public static function edit_fields_form($term, $taxonomy)
    {
        $image_url = get_term_meta($term->term_id, 'listing_make_image', true);
        $sort_order = get_term_meta($term->term_id, 'sort', true);
        $state = get_term_meta($term->term_id, 'state', true);
    ?>
        <tr class="form-field">
            <th scope="row" valign="top"><label for="sort"><?php esc_html_e('Sort Order', 'wp-cardealer'); ?></label></th>
            <td>
                <input type="number" name="sort" id="sort" value="<?php echo esc_attr($sort_order); ?>" />
                <p class="description"><?php esc_html_e('Enter a number to define the sort order for this make.', 'wp-cardealer'); ?></p>
            </td>
        </tr>

        <tr class="form-field">
            <th scope="row" valign="top"><label><?php esc_html_e('Image', 'wp-cardealer'); ?></label></th>
            <td>
                <div id="listing_make_image_preview" style="margin-bottom: 10px;">
                    <img src="<?php echo esc_url($image_url ? $image_url : get_stylesheet_directory_uri() . '/plugin/wp-cardealer/images/placeholder.jpg'); ?>" alt="<?php esc_attr_e('Image', 'wp-cardealer'); ?>" style="max-width: 150px; height: auto;" />
                </div>
                <input type="hidden" name="listing_make_image" id="listing_make_image" value="<?php echo esc_attr($image_url); ?>" />
                <button type="button" class="upload_image_button button"><?php esc_html_e('Upload Image', 'wp-cardealer'); ?></button>
            </td>
        </tr>

        <tr class="form-field">
            <th scope="row" valign="top"><label><?php esc_html_e('State', 'wp-cardealer'); ?></label></th>
            <td>
                <input type="radio" name="state" value="1" id="state_enable" <?php checked($state, '1'); ?> /> <label for="state_enable"><?php esc_html_e('Enable', 'wp-cardealer'); ?></label>
                <input type="radio" name="state" value="0" id="state_disable" <?php checked($state, '0'); ?> /> <label for="state_disable"><?php esc_html_e('Disable', 'wp-cardealer'); ?></label>
                <p class="description"><?php esc_html_e('Select whether this term is enabled or disabled.', 'wp-cardealer'); ?></p>
            </td>
        </tr>
<?php
    }

    public static function save($term_id, $tt_id, $taxonomy)
    {
        parent::save($term_id, $tt_id, $taxonomy);

        if (isset($_POST['sort'])) {
            $sort_order = intval($_POST['sort']);
            update_term_meta($term_id, 'sort', $sort_order);
        }

        if (isset($_POST['listing_make_image'])) {
            $image_url = esc_url_raw($_POST['listing_make_image']);
            update_term_meta($term_id, 'listing_make_image', $image_url);
        }

        if (isset($_POST['state'])) {
            $state = intval($_POST['state']);
            update_term_meta($term_id, 'state', $state); // Save the state value (1 for enabled, 0 for disabled)
        }
    }
}

Custom_Taxonomy_Car_Make::init();
