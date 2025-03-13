<?php
get_header();
$sidebar_configs = voiture_get_blog_layout_configs();
$current_url = trim(parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH), '/');
// print_r('motorcycle news page accessed...........');
error_log('motorcycle news page accessed...........');
echo do_shortcode('[breadcrumb]');
?>
<section>
    <div id="news-categories-tabs" class="news-categories-tabs">
        <div class="navbarcls">
            <!-- Scroll Left Button -->
            <div class="button-container-left">
                <button id="scroll-left" class="scroll-btn-left" aria-label="Scroll left" style="margin-right: 10px">
                    <i class="fas fa-chevron-left"></i>
                </button>
            </div>
            <!-- Category Tabs -->
            <ul id="category-tabs" class="category-tabs">
                <li class="tab-link <?php echo ($current_url == 'news-motorcycles/latest') ? 'selected' : ''; ?> <?php echo ($current_url == 'news-motorcycles') ? 'selected' : ''; ?>" data-category="0">ล่าสุด</li>
                <?php
                $categories = get_terms('motorcycle-news-category', array(
                    'hide_empty' => false,
                    'parent' => 0,
                    'meta_query' => array(
                        array(
                            'key'     => 'second_lang',
                            'value'   => '', // Empty value
                            'compare' => 'IN', // Either empty or not set
                        ),
                        array(
                            'key'     => 'state',
                            'value'   => '1', // Value of the 'state' field should be 1
                            'compare' => '=' // Exact match for state = 1
                        ),
                        array(
                            'key'     => 'type',
                            'value'   => '1', // Value of the 'type' field should be 1
                            'compare' => '=' // Exact match for type = 1
                        ),
                    ),
                    'orderby'    => 'meta_value', // Sort by term meta value
                    'meta_key'   => 'sort',
                    'order'      => 'ASC',
                ));
                foreach ($categories as $category) :
//                     $category_slug = sanitize_title($category->name);
					$category_slug = sanitize_title($category->slug);
                    $category_url = 'news-motorcycles/' . $category_slug;
                    $is_selected = ($current_url == $category_url) ? 'selected' : '';
                ?>
                    <li class="tab-link <?php echo $is_selected; ?>" data-category="<?php echo esc_attr($category->term_id); ?>">
                        <?php echo esc_html($category->name); ?>
                    </li>
                <?php endforeach; ?>
            </ul>


            <!-- Scroll Right Button -->
            <div class="button-container-right">
                <button id="scroll-right" class="scroll-btn-right" aria-label="Scroll right" style="margin-left: 20px">
                    <i class="fas fa-chevron-right"></i>
                </button>
            </div>
        </div>
    </div>
    <div id="subcategories-container" class="subcategories-container" style="margin-left: 103px">
    </div>
    <div id="loader" class="loader">
        <i class="spinner"></i>
    </div>
    <input type="hidden" name="parent-category" id="selected-category-id" value=""></input>
</section>
<section id="main-container" class="main-content <?php echo apply_filters('voiture_blog_content_class', 'container'); ?> inner">
    <?php voiture_before_content($sidebar_configs); ?>
    <div class="row responsive-medium archive-news-col ">
        <?php voiture_display_sidebar_left($sidebar_configs); ?>
        <div id="main-content" class="main-blog col-sm-12 <?php echo esc_attr($sidebar_configs['main']['class']); ?>">
            <div id="main" class="site-main layout-blog" role="main">
                <div id="news-articles">
                    <?php if (have_posts()) : ?>
                        <header class="page-header hidden">
                            <?php
                            the_archive_title('<h1 class="page-title">', '</h1>');
                            the_archive_description('<div class="taxonomy-description">', '</div>');
                            ?>
                        </header>
                    <?php
                    // $layout = voiture_get_config( 'blog_display_mode', 'list' );
                    // get_template_part( 'template-posts/layouts/'.$layout);
                    // voiture_paging_nav();
                    else :
                        get_template_part('template-posts/content', 'none');
                    endif;
                    ?>
                </div>
                <div style="display: flex; justify-content:center; text-align: center;">
                    <a href="#" id="load-more-link" style="display:none; text-decoration:none; font-size:14px; color:#576b95; display:block; text-align:center; margin:20px 0;font-weight:700">
                        View More <i class="fas fa-chevron-down" style="margin-left:5px;"></i>
                    </a>
                    <div id="loader" style="display:none; margin:20px 0;">
                        <i class="fas fa-spinner fa-spin" style="font-size:20px; color:#576b95;"></i>
                    </div>
                </div>
            </div><!-- .site-main -->
        </div><!-- .content-area -->
        <!-- <h3> Popular Models</h3> -->
        <div class="col-sm-12 col-md-4 col-lg-3 col-xs-12 recommended-cars-wrapper">

            <?php echo do_shortcode('[recommended_motors]'); ?>
            <?php echo do_shortcode('[popular_bike_videos]'); ?>

        </div>

    </div>

    <?php echo do_shortcode('[elementor-template id="4891"]'); ?>
</section>
<style>
    .recommended-cars-wrapper {
        max-width: 300px;
        /* Adjust this width as per your design */
        margin: 0 auto;
        /* Center the content */
        width: 100%;
        /* Ensures it takes full width of its parent up to the max-width */
    }
	.archive-news-col{
		padding-top:50px !important;
	}
    .recommended-cars-wrapper.full-width {
        max-width: none;
        /* Allows the section to expand if needed */
        width: 100%;
        /* Forces the element to take up full width if explicitly needed */
    }
	@media screen and (max-width: 768px) {
		.recommended-cars-wrapper {
			max-width: 100% !important;
			margin: 0 auto;
			width: 100%;
		}
	}
</style>
<?php
// }
get_footer(); ?>