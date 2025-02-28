<?php
get_header();
$sidebar_configs = voiture_get_blog_layout_configs();
// voiture_render_breadcrumbs();
if ($post_type === 'mobil') {
    // Retrieve query variables for make, model, and section
    $make = get_query_var('make');
    $model = get_query_var('model');
    $section = get_query_var('section');
    $variant_section = get_query_var('variant_section');

    // if section is not equal to 'overview', 'news', 'specs', 'gallery', 'fuel-consumption', 'colors'
    // check if variant exists with that name
    $individual_pages = ['overview', 'news', 'specs', 'gallery', 'fuel-consumption', 'colors', ''];
    if (!in_array($section, $individual_pages)) {
        $variant_post = get_posts(array(
            'name' => $section,
            'post_type' => 'variant',
            'posts_per_page' => 1
        ));
        if (!empty($variant_post)) {
            $section = 'overview';
            $variant_section = $variant_post[0]->post_name;
        }
    }

    $elementor_page_id = 237535;
    $spec_spec_id = 237533;
    $news_id = 237531;
    $overview_id =  237529;

    $fuel_consumption_id = 327506;
    $color_page_id = 327512;


    // variant pages
    $variant_overview_page_id = 327477;
    $variant_specs_page_id = 327485;
    $variant_gallery_page_id = 327497;
	
?>

    <section id="main-container" class="main-content <?php echo apply_filters('voiture_blog_content_class', 'container'); ?> inner">
        <?php voiture_before_content($sidebar_configs); ?>
        <div class="row responsive-medium">
            <?php voiture_display_sidebar_left($sidebar_configs); ?>
            <div id="main-content" class="main-blog col-sm-12 ">

                <div id="main" class="site-main layout-blog" role="main">
                    <?php
                    switch ($section) {
                        case 'overview':
                            if ($variant_section) {
                                $elementor_query = new WP_Query(array('page_id' => $variant_overview_page_id));
                                if ($elementor_query->have_posts()) :
                                    while ($elementor_query->have_posts()) : $elementor_query->the_post();
                                        the_content(); // Display Elementor page content
                                    endwhile;
                                    wp_reset_postdata();
                                else :
                                    echo '<p>No content found for the variant cars archive.</p>';
                                endif;
                                break;
                            }

                            $elementor_query = new WP_Query(array('page_id' => $overview_id));
                            if ($elementor_query->have_posts()) :
                                while ($elementor_query->have_posts()) : $elementor_query->the_post();
                                    the_content(); // Display Elementor page content
                                endwhile;
                                wp_reset_postdata();
                            else :
                                echo '<p>No content found for the cars archive.</p>';
                            endif;
                            break;

                        case 'news':
                            //echo WP_CarDealer_Template_Loader::get_template_part('single-listing/news');

                            $elementor_query = new WP_Query(array('page_id' => $news_id));
                            if ($elementor_query->have_posts()) :
                                while ($elementor_query->have_posts()) : $elementor_query->the_post();
                                    the_content(); // Display Elementor page content
                                endwhile;
                                wp_reset_postdata();
                            else :
                                echo '<p>No content found for the cars archive.</p>';
                            endif;
                            break;

                        case 'gallery':
                            if ($variant_section) {
                                $elementor_query = new WP_Query(array('page_id' => $variant_gallery_page_id));
                                if ($elementor_query->have_posts()) :
                                    while ($elementor_query->have_posts()) : $elementor_query->the_post();
                                        the_content(); // Display Elementor page content
                                    endwhile;
                                    wp_reset_postdata();
                                else :
                                    echo '<p>No content found for the variant cars archive.</p>';
                                endif;
                                break;
                            }

                            $elementor_query = new WP_Query(array('page_id' => $elementor_page_id));
                            if ($elementor_query->have_posts()) :
                                while ($elementor_query->have_posts()) : $elementor_query->the_post();
                                    the_content(); // Display Elementor page content
                                endwhile;
                                wp_reset_postdata();
                            else :
                                echo '<p>No content found for the cars archive.</p>';
                            endif;
                            break;

                        case 'specs':
                            if ($variant_section) {
                                $elementor_query = new WP_Query(array('page_id' => $variant_specs_page_id));
                                if ($elementor_query->have_posts()) :
                                    while ($elementor_query->have_posts()) : $elementor_query->the_post();
                                        the_content(); // Display Elementor page content
                                    endwhile;
                                    wp_reset_postdata();
                                else :
                                    echo '<p>No content found for the variant cars archive.</p>';
                                endif;
                                break;
                            }

                            $elementor_query = new WP_Query(array('page_id' => $spec_spec_id));
                            if ($elementor_query->have_posts()) :
                                while ($elementor_query->have_posts()) : $elementor_query->the_post();
                                    the_content(); // Display Elementor page content
                                endwhile;
                                wp_reset_postdata();
                            else :
                                echo '<p>No content found for the cars archive.</p>';
                            endif;

                            // echo WP_CarDealer_Template_Loader::get_template_part('single-listing/spec');
                            break;
                        case 'fuel-consumption':
                            $elementor_query = new WP_Query(array('page_id' => $fuel_consumption_id));
                            if ($elementor_query->have_posts()) :
                                while ($elementor_query->have_posts()) : $elementor_query->the_post();
                                    the_content(); // Display Elementor page content
                                endwhile;
                                wp_reset_postdata();
                            else :
                                echo '<p>No content found for the cars archive. - fuel consumption</p>';
                            endif;
                            // echo WP_CarDealer_Template_Loader::get_template_part('single-listing/fuel-consumption');
                            break;

                        case 'colors':
                            $elementor_query = new WP_Query(array('page_id' => $color_page_id));
                            if ($elementor_query->have_posts()) :
                                while ($elementor_query->have_posts()) : $elementor_query->the_post();
                                    the_content(); // Display Elementor page content
                                endwhile;
                                wp_reset_postdata();
                            else :
                                echo '<p>No content found for the cars archive.</p>';
                            endif;
                            // echo WP_CarDealer_Template_Loader::get_template_part('single-listing/color');
                            break;
                        default:
                            $elementor_query = new WP_Query(array('page_id' => $overview_id));
                            if ($elementor_query->have_posts()) :
                                while ($elementor_query->have_posts()) : $elementor_query->the_post();
                                    the_content(); // Display Elementor page content
                                endwhile;
                                wp_reset_postdata();
                            else :
                                echo '<p>No content found for the cars archive.</p>';
                            endif;
                            // echo WP_CarDealer_Template_Loader::get_template_part('single-listing/overview');
                    }
                    ?>
                </div><!-- .site-main -->
            </div><!-- .content-area -->
        </div>
    </section>
<?php
} else {
?>
    <section id="main-container" class="main-content <?php echo apply_filters('voiture_blog_content_class', 'container'); ?> inner">
        <?php
        $current_url = $_SERVER['REQUEST_URI'];
        $path_parts = explode('/', trim($current_url, '/'));
        // $last_part = end($path_parts);
        $is_search = 0;

        $last_part = get_query_var('news_slug');

        // Check if the last part matches the pattern (contains numbers at the end)
        if (preg_match('/(.*)-(\d+)$/', $last_part, $matches)) {
            // Get the post_name without the number
            $post_name = $matches[1];
            $news_id = $matches[2];
        } else {
            $post_name = $last_part;
            $is_search = 1;
        }
        global $wpdb;
        $result = $wpdb->get_var(
            $wpdb->prepare("SELECT news_post_id FROM news_temp WHERE news_id = %d", $news_id)
        );

        $current_post_id = $result ? $result : $news_id;
        $args = array(
            'p' => $current_post_id,
            'post_type' => 'news',
            'post_status' => 'publish',
            'posts_per_page' => 1
        );
        $query = new WP_Query($args);

        if (!empty($query->posts)) {
            $found_post = $query->posts[0];
            $found_post_name = $found_post->post_name;

            // Strict comparison of post_name
            // if ($found_post_name !== $post_name) {
            //     $is_search = 1;
            // }
        } else {
            $is_search = 1;
        }
        if ($is_search !== 0) {
            // If post doesn't exist, redirect to search page
            $search_url = home_url('/?s=' . $post_name);
            wp_redirect($search_url);
            exit;
        }

        // if mobile device
        //         if (is_mobile_device()) {
        //             echo do_shortcode('[current_post_data is_amp=1]');
        //         } else {
        $new_individual_page = 237531;
        $elementor_query = new WP_Query(array('page_id' => $new_individual_page));
        if ($elementor_query->have_posts()) :
            while ($elementor_query->have_posts()) : $elementor_query->the_post();
                the_content();
            endwhile;
            wp_reset_postdata();
        else :
            echo '<p>No content found for the cars archive.</p>';
        endif;
        //         }

        ?>
    </section>
<?php
}
get_footer();


function is_mobile_device()
{
    return preg_match('/(android|webos|iphone|ipad|ipod|blackberry|iemobile|opera mini)/i', $_SERVER['HTTP_USER_AGENT']);
}

?>